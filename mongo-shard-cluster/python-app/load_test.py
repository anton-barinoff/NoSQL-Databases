#!/usr/bin/env python3
from pymongo import MongoClient, errors
import threading
import time
import random
from datetime import datetime
import statistics
import os
import sys

def wait_for_mongodb(uri, max_attempts=30, delay=2):
    """Ожидание готовности MongoDB."""
    print(f"Connecting {uri}...")
    for attempt in range(max_attempts):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=2000)
            client.admin.command('ping')
            print("MongoDB is ready")
            return client
        except errors.ServerSelectionTimeoutError:
            print(f"Waiting for MongoDB. Attemt {attempt + 1}/{max_attempts}")
            time.sleep(delay)
    raise Exception("Cannot connect to MongoDB")

class LoadTester:
    def __init__(self):
        uri = os.getenv("MONGO_URI", "mongodb://mongos:27017/")
        print(f"Initializing LoadTester with URI: {uri}")
        self.client = wait_for_mongodb(uri)
        self.db = self.client["etl_db"]
    
    def worker_read(self, worker_id, duration, results):
        """Чтение данных."""
        end_time = time.time() + duration
        count = 0
        latencies = []
        
        while time.time() < end_time:
            try:
                user_id = f"user_{random.randint(1, 50)}"
                
                start = time.time()
                list(self.db.movie_views.find({"user_id": user_id}).limit(5))
                latency = (time.time() - start) * 1000
                
                count += 1
                latencies.append(latency)
            except:
                pass
        
        results.append({
            "worker": worker_id,
            "type": "read",
            "ops": count,
            "avg_latency": statistics.mean(latencies) if latencies else 0
        })
    
    def worker_write(self, worker_id, duration, results):
        """Запись данных."""
        end_time = time.time() + duration
        count = 0
        latencies = []
        
        movies = [
            "movie_101", "movie_102", "movie_103", "movie_104", "movie_105", 
            "movie_106", "movie_107", "movie_108", "movie_109", "movie_110"
            ]
        
        while time.time() < end_time:
            try:
                view = {
                    "view_id": f"load_test_{worker_id}_{count}_{int(time.time())}",
                    "user_id": f"user_{random.randint(1, 50)}",
                    "movie_id": random.choice(movies),
                    "movie_title": "Load Test Movie",
                    "start_time": datetime.now().isoformat() + "Z",
                    "watch_duration_minutes": random.randint(30, 150)
                }
                
                start = time.time()
                self.db.movie_views.insert_one(view)
                latency = (time.time() - start) * 1000
                
                count += 1
                latencies.append(latency)
            except:
                pass
        
        results.append({
            "worker": worker_id,
            "type": "write",
            "ops": count,
            "avg_latency": statistics.mean(latencies) if latencies else 0
        })
    
    def run_test(self, duration=10, readers=2, writers=2):
        """Запуск теста."""
        print(f"Duration: {duration} sec")
        print(f"Readers: {readers}")
        print(f"Writers: {writers}")
        
        threads = []
        results = []
        
        for i in range(readers):
            t = threading.Thread(target=self.worker_read, args=(f"R{i}", duration, results))
            t.start()
            threads.append(t)
        
        for i in range(writers):
            t = threading.Thread(target=self.worker_write, args=(f"W{i}", duration, results))
            t.start()
            threads.append(t)
        
        for t in threads:
            t.join()
        
        read_ops = sum(r["ops"] for r in results if r["type"] == "read")
        write_ops = sum(r["ops"] for r in results if r["type"] == "write")
        read_latency = statistics.mean([r["avg_latency"] for r in results if r["type"] == "read"]) if read_ops else 0
        write_latency = statistics.mean([r["avg_latency"] for r in results if r["type"] == "write"]) if write_ops else 0
        
        print("Results:")
        print(f"   Reading: {read_ops} ops ({read_ops/duration:.2f} ops/sec)")
        print(f"   Avg delay reading: {read_latency:.2f} ms")
        print(f"   Writing: {write_ops} ops ({write_ops/duration:.2f} ops/sec)")
        print(f"   Avg delay writing: {write_latency:.2f} ms")
        print(f"   Total: {read_ops + write_ops} ops ({(read_ops+write_ops)/duration:.2f} ops/sec)")
        
        return {
            "read_ops": read_ops,
            "write_ops": write_ops,
            "read_latency": read_latency,
            "write_latency": write_latency
        }

if __name__ == "__main__":
    tester = LoadTester()
    print("Test 1: Reading only")
    tester.run_test(duration=5, readers=2, writers=0)
    
    print("\nTest 2: Writing only")
    tester.run_test(duration=5, readers=0, writers=2)
    
    print("\nTest 3: Mixed")
    tester.run_test(duration=10, readers=2, writers=2)