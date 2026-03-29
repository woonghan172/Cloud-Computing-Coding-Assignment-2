import os
os.environ['PYTHONWARNINGS'] = 'ignore'

import warnings
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", message="The 'strict' parameter is no longer needed")

import threading
import logging
import unittest
import requests

class TestKVStoreConcurrency(unittest.TestCase):
    # Set the base URL where the FastAPI server is running
    BASE_URL = "http://127.0.0.1:8080"
    TEST_KEY = "concurrency_test_key"

    def setUp(self):
        """
        Executed before each test: 
        Ensure a clean state by deleting the test key.
        """
        print("\n[System] Setting up the test environment...")
        requests.delete(f"{self.BASE_URL}/{self.TEST_KEY}")

    # def tearDown(self):
    #     """
    #     Executed after each test: 
    #     Cleanup by deleting the test key.
    #     """
    #     print("[System] Cleaning up the test environment...")
    #     requests.delete(f"{self.BASE_URL}/{self.TEST_KEY}")

    def test_concurrent_put_requests(self):
        """
        Requirement: Handle multiple concurrent requests.
        Verifies if the server handles 50 simultaneous PUT requests without race conditions.
        """
        num_threads = 50
        results = []
        
        # This list will store any exceptions occurred during thread execution
        exceptions = []

        def worker(thread_id):
            try:
                # Each thread attempts to update the same key with a unique value
                payload = {"value": f"value_{thread_id}"}
                response = requests.post(f"{self.BASE_URL}/{self.TEST_KEY}", json=payload, timeout=5)
                results.append(response.status_code)
            except Exception as e:
                exceptions.append(e)

        # 1. Initialize and start multiple threads to simulate high concurrency
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        
        for t in threads:
            t.start()

        # 2. Wait for all threads to finish their execution
        for t in threads:
            t.join()

        # 3. Assertions for reliability
        # Check if there were any connection or timeout exceptions
        self.assertEqual(len(exceptions), 0, f"Exceptions occurred during concurrent requests: {exceptions}")
        
        # Check if all 50 requests were successful (HTTP 200)
        self.assertEqual(len(results), num_threads)
        for status in results:
            self.assertEqual(status, 200, f"A request failed with status code: {status}")

        # 4. Final state verification
        # Retrieve the final value to ensure the store and file are not corrupted
        final_response = requests.get(f"{self.BASE_URL}/{self.TEST_KEY}")
        self.assertEqual(final_response.status_code, 200)
        
        # The value should be one of the 'value_i' strings, proving data integrity
        self.assertTrue(final_response.json()["value"].startswith("value_"))

    def test_error_handling_non_existent_key(self):
        """
        Requirement: Error Handling.
        Verifies if the server returns a 404 error for a non-existent key.
        """
        response = requests.get(f"{self.BASE_URL}/this_key_does_not_exist")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["detail"], "Key not found")

if __name__ == "__main__":
    # Start the test suite
    unittest.main()