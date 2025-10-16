import sys
import json
import random

def main():
    try:
        input_data = sys.stdin.read()
        data = json.loads(input_data)

        job_description = data["job_description"]
        system_description = data["hpc_system_description"]
        system_status = data["hpc_system_status"]

        # Do not modify before here
        # Add logic to generate a bid based on some the job and system descriptions and system status here
        
        bid = 0.0

        # Do not modify after here
        result = {
            "bid": bid,
        }

        print(json.dumps(result))
    except Exception as e:
        print(json.dumps({"error": str(e)}))

if __name__ == "__main__":
    main()
