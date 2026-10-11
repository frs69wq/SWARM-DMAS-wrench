#ifndef PYTHON_RUNNER_H
#define PYTHON_RUNNER_H

#include <nlohmann/json.hpp>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>

// Keep one Python child alive for multiple newline-delimited JSON request/response exchanges.
class PersistentPythonWorker {
  std::string script_path_;
  int socket_fd_ = -1;
  pid_t pid_ = -1;
  std::string pending_output_;
  bool request_pending_ = false;

  void stop(bool terminate) noexcept
  {
    if (socket_fd_ >= 0) {
      if (!terminate) {
        // EOF tells the worker's input loop to exit during normal shutdown.
        shutdown(socket_fd_, SHUT_WR);
      }
      close(socket_fd_);
      socket_fd_ = -1;
    }

    if (pid_ > 0) {
      if (terminate)
        kill(pid_, SIGTERM);
      int status;
      while (waitpid(pid_, &status, 0) == -1 && errno == EINTR) {
      }
      pid_ = -1;
    }

    pending_output_.clear();
    request_pending_ = false;
  }

  void start()
  {
    if (pid_ > 0)
      return;
    if (access(script_path_.c_str(), F_OK) != 0)
      throw std::runtime_error("Python script not found: " + script_path_);

    int sockets[2];
    // Prevent concurrently started workers from inheriting each other's sockets across exec.
    if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sockets) == -1)
      throw std::runtime_error("Failed to create Python worker socket");

    pid_t child_pid = fork();
    if (child_pid == -1) {
      close(sockets[0]);
      close(sockets[1]);
      throw std::runtime_error("Failed to start Python worker");
    }
    if (child_pid == 0) {
      close(sockets[0]);
      if (dup2(sockets[1], STDIN_FILENO) == -1 || dup2(sockets[1], STDOUT_FILENO) == -1) {
        perror("dup2 failed");
        _exit(1);
      }
      close(sockets[1]);
      execlp("python3", "python3", "-u", script_path_.c_str(), nullptr);
      perror("execlp failed");
      _exit(1);
    }

    close(sockets[1]);
    socket_fd_ = sockets[0];
    pid_ = child_pid;
  }

public:
  explicit PersistentPythonWorker(std::string script_path) : script_path_(std::move(script_path)) {}
  PersistentPythonWorker(const PersistentPythonWorker&) = delete;
  PersistentPythonWorker& operator=(const PersistentPythonWorker&) = delete;

  ~PersistentPythonWorker() { stop(false); }

  void send_request(const nlohmann::json& request)
  {
    if (request_pending_)
      throw std::runtime_error("Python worker already has an unread response");
    start();

    const std::string payload = request.dump() + "\n";
    size_t sent = 0;
    while (sent < payload.size()) {
      ssize_t count = send(socket_fd_, payload.data() + sent, payload.size() - sent, MSG_NOSIGNAL);
      if (count <= 0) {
        stop(true);
        throw std::runtime_error("Failed to send request to Python worker");
      }
      sent += static_cast<size_t>(count);
    }
    request_pending_ = true;
  }

  nlohmann::json read_response()
  {
    if (!request_pending_)
      throw std::runtime_error("Python worker has no pending request");

    char buffer[4096];
    while (true) {
      size_t newline = pending_output_.find('\n');
      if (newline != std::string::npos) {
        std::string response = pending_output_.substr(0, newline);
        pending_output_.erase(0, newline + 1);
        request_pending_ = false;
        try {
          return nlohmann::json::parse(response);
        } catch (const std::exception& e) {
          throw std::runtime_error(std::string("Failed to parse Python worker response: ") + e.what());
        }
      }

      ssize_t count = recv(socket_fd_, buffer, sizeof(buffer), 0);
      if (count <= 0) {
        stop(true);
        throw std::runtime_error("Python worker exited before returning a response");
      }
      pending_output_.append(buffer, static_cast<size_t>(count));
    }
  }

  nlohmann::json request(const nlohmann::json& input)
  {
    send_request(input);
    return read_response();
  }
};

inline bool uses_persistent_claude_worker(const std::string& script_path)
{
  const size_t separator = script_path.find_last_of("/\\");
  return script_path.substr(separator == std::string::npos ? 0 : separator + 1) == "llm_claude_bidder.py";
}

/**
 * @brief Execute a Python script with JSON input and return JSON output.
 *
 * @param script_path Path to the Python script to execute.
 * @param input JSON object to pass to the script via stdin.
 * @return nlohmann::json The parsed JSON response from the script.
 * @throws std::runtime_error if pipes fail, script not found, or JSON parsing fails.
 */
inline nlohmann::json run_python_script(const std::string& script_path, const nlohmann::json& input)
{
  int to_python[2];
  int from_python[2];

  if (pipe(to_python) == -1 || pipe(from_python) == -1)
    throw std::runtime_error("Failed to create pipes");

  if (access(script_path.c_str(), F_OK) != 0)
    throw std::runtime_error("Python script not found: " + script_path);

  pid_t pid = fork();
  if (pid == 0) {
    // Child process: become the Python interpreter
    dup2(to_python[0], STDIN_FILENO);
    dup2(from_python[1], STDOUT_FILENO);

    close(to_python[1]);
    close(from_python[0]);
    close(to_python[0]);
    close(from_python[1]);

    execlp("python3", "python3", script_path.c_str(), nullptr);
    perror("execlp failed");
    exit(1);
  } else {
    // Parent process: communicate with Python
    close(to_python[0]);
    close(from_python[1]);

    // Send JSON input to Python
    std::string jsonStr = input.dump();
    write(to_python[1], jsonStr.c_str(), jsonStr.size());
    close(to_python[1]); // Signal EOF to Python

    // Read JSON response from Python
    std::string response;
    char buffer[256];
    ssize_t count;
    while ((count = read(from_python[0], buffer, sizeof(buffer) - 1)) > 0) {
      buffer[count] = '\0';
      response += buffer;
    }
    close(from_python[0]);
    waitpid(pid, nullptr, 0);

    try {
      return nlohmann::json::parse(response);
    } catch (const std::exception& e) {
      throw std::runtime_error(std::string("Failed to parse Python response: ") + e.what() +
                               "\nResponse was: " + response);
    }
  }
}

#endif // PYTHON_RUNNER_H
