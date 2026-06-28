import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configurable parameters
TEST_URLS = {
    "vci": "https://board.vcsc.com.vn/rest/api/v1/finance/price/history?symbol=VNM&resolution=D&from=1704067200&to=1704153600",
    "kbs": "https://kbsec-price.kbsec.com.vn/api/v1/chart/history?symbol=VNM&resolution=1D&from=1704067200&to=1704153600"
}
NUM_REQUESTS = 20
CONCURRENCY = 4

def send_request(source: str, url: str) -> bool:
    """Send a single HTTP GET request to check response code and performance."""
    start_time = time.time()
    try:
        response = requests.get(url, timeout=10)
        duration = time.time() - start_time
        if response.status_code == 200:
            logger.info("Request successful for source=%s (duration=%.2fs)", source, duration)
            return True
        else:
            logger.warning("Request failed for source=%s (status_code=%d, duration=%.2fs)", 
                           source, response.status_code, duration)
            return False
    except Exception as e:
        duration = time.time() - start_time
        logger.error("Request exception for source=%s: %s (duration=%.2fs)", source, str(e), duration)
        return False

def main():
    logger.info("Starting Ingestion Throughput Benchmark...")
    logger.info("Configuration: requests=%d, concurrency=%d", NUM_REQUESTS, CONCURRENCY)
    
    start_all = time.time()
    success_count = 0
    
    # We will query KBS and VCI alternately
    urls_pool = []
    for i in range(NUM_REQUESTS):
        source = "vci" if i % 2 == 0 else "kbs"
        urls_pool.append((source, TEST_URLS[source]))
        
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [executor.submit(send_request, src, url) for src, url in urls_pool]
        for fut in as_completed(futures):
            if fut.result():
                success_count += 1
                
    total_duration = time.time() - start_all
    throughput_per_minute = (success_count / total_duration) * 60 if total_duration > 0 else 0
    
    logger.info("=== BENCHMARK SUMMARY ===")
    logger.info("Total Requests Sent : %d", NUM_REQUESTS)
    logger.info("Successful Requests : %d", success_count)
    logger.info("Total Duration      : %.2f seconds", total_duration)
    logger.info("Calculated Throughput: %.2f requests/minute", throughput_per_minute)

if __name__ == "__main__":
    main()
