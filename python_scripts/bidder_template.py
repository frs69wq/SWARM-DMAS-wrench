import sys
import json
import time

def main():
    try:
        input_data = sys.stdin.read()
        data = json.loads(input_data)

        job_description = data["job_description"]
        system_description = data["hpc_system_description"]
        system_status = data["hpc_system_status"]

        # Start timing
        start_time = time.perf_counter()

        # Do not modify before here
        # Add logic to generate a bid based on the job and system descriptions and system status here
        bid = 0.0

        # End timing
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time

        # Do not modify after here
        result = {
            "bid": bid,
            "bid_generation_time_seconds": round(elapsed_time, 6)
        }

        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    main()
