import threading

# Set by the download path when it finds a bad replica.
# Step 10's repair monitor will wait() on this instead of only sleeping.
repair_wakeup = threading.Event()