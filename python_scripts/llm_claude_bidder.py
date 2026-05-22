import sys
import json
import os
import time
import logging
import re
from HeuristicBidding import compute_bid
#################################
# Claude OpenAI imports 
from anthropic import AnthropicVertex
from google.oauth2 import service_account
#################################
# Model Setup
CLAUDE_LOCATION   = os.getenv("CLAUDE_LOCATION")
CLAUDE_PROJECT_ID = os.getenv("CLAUDE_PROJECT_ID")
CLAUDE_MODEL      = os.getenv("CLAUDE_MODEL")
TEMP = 0
MAX_TOKENS = 5000
os.environ["ANTHROPIC_LOG"] = "debug"
#################################
# Logs - provide a log_path here
log_path = os.environ.get(
    "LLM_LOG_FILE",
    "/workspaces/SWARM-DMAS-wrench/logs/business_large_long_rho0.9.log"
)
os.makedirs(os.path.dirname(log_path), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler(log_path)
    ]
)
logger = logging.getLogger(__name__)
#################################
# Feasibility check function to short-circuit obviously infeasible jobs before calling the LLM
def is_feasible_system(job_description, system_description):
    nodes_req = job_description.get("num_nodes")
    req_gpu = job_description.get("needs_gpu")
    req_mem = job_description.get("requested_memory_gb")
    req_storage = job_description.get("requested_storage_gb")

    sys_nodes = system_description.get("num_nodes")
    sys_has_gpu = system_description.get("has_gpu")
    sys_mem_per_node = system_description.get("memory_amount_in_gb")
    sys_total_storage = system_description.get("storage_amount_in_gb", float("inf"))

    if nodes_req is not None and sys_nodes is not None and nodes_req > sys_nodes:
        return False, "nodes"
    if req_gpu and not sys_has_gpu:
        return False, "gpu"
    if req_mem is not None and sys_mem_per_node is not None and sys_nodes is not None:
        if req_mem > (sys_mem_per_node * sys_nodes):
            return False, "memory"
    if req_storage is not None and req_storage > sys_total_storage:
        return False, "storage"

    return True, ""

def main():
    start_time = time.perf_counter()
    llm_response = ""
    job_description = None
    system_description = None
    system_status = None

    try:
        input_data = sys.stdin.read()
        data = json.loads(input_data)

        job_description = data["job_description"]
        system_description = data["hpc_system_description"]
        system_status = data["hpc_system_status"]
        runtime_prompt = data.get("prompt")

        # Start timing
        start_time = time.perf_counter()

        # Console-only status line requested by user
        job_id = job_description.get("job_id", "unknown")
        system_name = system_description.get("name", "unknown")
        print(f"Processing job {job_id} on machine {system_name}", file=sys.stderr, flush=True)

        feasible, infeasible_reason = is_feasible_system(job_description, system_description)
        if not feasible:
            elapsed_time = time.perf_counter() - start_time
            result = {
                "bid": 0.0,
                "bid_generation_time_seconds": round(elapsed_time, 6)
            }
            logger.info("Feasibility check failed (%s); returning zero bid", infeasible_reason)
            logger.info("Result: %s", json.dumps(result))
            print(json.dumps(result))
            return

        # Do not modify before here
        # Step1: Prompt instructions
        if not isinstance(runtime_prompt, str) or not runtime_prompt.strip():
            raise ValueError("No prompt provided. Set 'bidder_prompt_file' in the experiment config.")

        prompt = (
            runtime_prompt
            .replace("{job_description}", json.dumps(job_description, indent=2))
            .replace("{system_description}", json.dumps(system_description, indent=2))
            .replace("{system_status}", json.dumps(system_status, indent=2))
        )

        # job id, system status, system name instead of long prompt in logs to save space
        logger.info("Prompt for job %s submitted on system %s with status %s", job_description.get("job_id", "unknown"), system_description.get("name", "unknown"), system_status)
        # logger.info("Prompt:\n%s", prompt)

        # Step2: Format prompt
        message = [{"role": "user", "content": prompt}]

        # Step3: Setup Openai client
        client = AnthropicVertex(region=CLAUDE_LOCATION, project_id=CLAUDE_PROJECT_ID)

        # Step4: Get completion
        llm_response = client.messages.create(
            model=CLAUDE_MODEL,
            messages=message,
            max_tokens=MAX_TOKENS,
            temperature=TEMP,
        )
        for content_block in llm_response.content:
            llm_response = content_block.text

        # if LOG_PROMPT_AND_RESPONSE:
        logger.info("Response:\n%s", llm_response)

    except Exception as e:
        logger.error("Claude bidder request failed; will fall back to heuristic: %s", e)

    # End timing
    end_time = time.perf_counter()
    elapsed_time = end_time - start_time

    # If any of the critical inputs are missing return zero bid immediately
    if job_description is None or system_description is None or system_status is None:
        result = {
            "bid": 0.0,
            "bid_generation_time_seconds": round(elapsed_time, 6)
        }
        logger.error("Input unavailable after exception; returning zero bid")
        print(json.dumps(result))
        return

    # Step5: Response Parsing
    bid_score_pattern = r'"bid_score":\s*([0-9]*\.?[0-9]+)'
    match = re.search(bid_score_pattern, llm_response)

    # Step6: Fallback to the Heuristic bid if parsing fails
    heuristic_bid = compute_bid(job_description, system_description, system_status)

    # Log both heuristic and LLM bids for transparency, even if LLM parsing fails
    logger.info("Job %s: Heuristic bid=%.4f, LLM bid=%s", job_description.get("job_id", "unknown"), heuristic_bid, match.group(1) if match else "parsing_failed")
    
    # Log if LLM or Heuristic used
    if match:
        logger.info("Job %s: Using LLM bid", job_description.get("job_id", "unknown"))
    else:
        logger.info("Job %s: Using Heuristic bid", job_description.get("job_id", "unknown"))

    bid = float(match.group(1)) if match else heuristic_bid

    # Do not modify after here
    result = {
      "bid": bid,
      "bid_generation_time_seconds": round(elapsed_time, 6)
    }
    logger.info(json.dumps(result))
    print(json.dumps(result))

if __name__ == "__main__":
    main()
