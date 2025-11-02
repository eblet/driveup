# -*- coding: utf-8 -*-

import logging
import threading
import time
from typing import Optional

log = logging.getLogger(__name__)


class AdaptiveRateLimiter:
    """
    Adaptive rate limiter for Google API calls with SSL error protection.
    
    Features:
    - Throttles concurrent API calls using a semaphore
    - Automatically backs off when SSL errors are detected
    - Adapts rate limits based on error frequency
    """
    
    def __init__(self, max_concurrent_calls: int = 2, min_delay: float = 0.1):
        """
        Initialize the rate limiter.
        
        Args:
            max_concurrent_calls: Maximum number of concurrent API calls
            min_delay: Minimum delay between API calls (seconds)
        """
        self.semaphore = threading.Semaphore(max_concurrent_calls)
        self.min_delay = min_delay
        self.last_call_time = {}  # Track last call time per thread
        self.lock = threading.Lock()
        
        # Adaptive throttling
        self.ssl_error_count = 0
        self.total_calls = 0
        self.adaptive_delay = min_delay
        self.last_ssl_error_time = 0
        
        # Configuration
        self.ssl_error_threshold = 10  # Increase delay after N SSL errors (was 3, too aggressive)
        self.max_adaptive_delay = 10.0  # Maximum adaptive delay (seconds)
        self.recovery_time = 120  # Time to reduce delay if no errors (seconds)
        
        log.info(f"🔧 AdaptiveRateLimiter initialized: max_concurrent={max_concurrent_calls}, min_delay={min_delay}s")
    
    def acquire(self, timeout: Optional[float] = None) -> bool:
        """
        Acquire permission to make an API call.
        
        Args:
            timeout: Maximum time to wait for permission (None = wait forever)
            
        Returns:
            True if acquired, False if timeout
        """
        # Try to acquire semaphore (limit concurrent calls)
        acquired = self.semaphore.acquire(timeout=timeout)
        if not acquired:
            return False
        
        # Apply adaptive delay
        thread_id = threading.current_thread().ident
        
        with self.lock:
            self.total_calls += 1
            
            # Check if we should reduce delay (recovery)
            if self.ssl_error_count > 0 and (time.time() - self.last_ssl_error_time) > self.recovery_time:
                old_delay = self.adaptive_delay
                self.adaptive_delay = max(self.min_delay, self.adaptive_delay * 0.8)
                self.ssl_error_count = max(0, self.ssl_error_count - 1)
                log.info(f"📉 Reducing adaptive delay: {old_delay:.2f}s → {self.adaptive_delay:.2f}s (recovery)")
            
            # Get last call time for this thread
            last_time = self.last_call_time.get(thread_id, 0)
            current_time = time.time()
            
            # Calculate required delay
            elapsed = current_time - last_time
            required_delay = self.adaptive_delay
            
            if elapsed < required_delay:
                sleep_time = required_delay - elapsed
                self.last_call_time[thread_id] = current_time + sleep_time
            else:
                self.last_call_time[thread_id] = current_time
        
        # Sleep outside the lock to avoid blocking other threads
        if elapsed < required_delay:
            time.sleep(sleep_time)
        
        return True
    
    def release(self):
        """Release the semaphore after API call completes."""
        self.semaphore.release()
    
    def report_ssl_error(self):
        """Report an SSL error to trigger adaptive throttling."""
        with self.lock:
            self.ssl_error_count += 1
            self.last_ssl_error_time = time.time()
            
            # Increase delay if error threshold exceeded
            if self.ssl_error_count >= self.ssl_error_threshold:
                old_delay = self.adaptive_delay
                self.adaptive_delay = min(self.max_adaptive_delay, self.adaptive_delay * 1.5)
                
                if old_delay != self.adaptive_delay:
                    log.warning(
                        f"🔥 SSL error threshold reached ({self.ssl_error_count} errors)! "
                        f"Increasing delay: {old_delay:.2f}s → {self.adaptive_delay:.2f}s"
                    )
            else:
                log.debug(f"SSL error reported ({self.ssl_error_count}/{self.ssl_error_threshold})")
    
    def report_success(self):
        """Report a successful API call."""
        # Success doesn't immediately reduce delay, but helps with recovery tracking
        pass
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        with self.lock:
            return {
                'total_calls': self.total_calls,
                'ssl_errors': self.ssl_error_count,
                'current_delay': self.adaptive_delay,
                'error_rate': self.ssl_error_count / max(1, self.total_calls) * 100
            }
    
    def __enter__(self):
        """Context manager support."""
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.release()
        
        # Report SSL errors to trigger adaptive throttling
        if exc_type is not None and 'SSL' in str(exc_type):
            self.report_ssl_error()
        elif exc_type is None:
            self.report_success()
        
        return False  # Don't suppress exceptions


# Global rate limiter instance (will be initialized in main.py)
_global_rate_limiter: Optional[AdaptiveRateLimiter] = None


def get_rate_limiter() -> AdaptiveRateLimiter:
    """Get the global rate limiter instance."""
    global _global_rate_limiter
    if _global_rate_limiter is None:
        # Default configuration for single-threaded mode
        _global_rate_limiter = AdaptiveRateLimiter(max_concurrent_calls=1, min_delay=0.05)
    return _global_rate_limiter


def init_rate_limiter(max_workers: int = 1, min_delay: float = 0.2):
    """
    Initialize the global rate limiter.
    
    Args:
        max_workers: Number of parallel workers
        min_delay: Minimum delay between API calls
    """
    global _global_rate_limiter
    
    # Adjust concurrent calls based on workers
    # Allow more concurrent calls than workers to avoid blocking
    max_concurrent_calls = max(2, max_workers * 2)
    
    _global_rate_limiter = AdaptiveRateLimiter(
        max_concurrent_calls=max_concurrent_calls,
        min_delay=min_delay
    )
    
    log.info(f"🚀 Rate limiter initialized for {max_workers} workers")
    return _global_rate_limiter

