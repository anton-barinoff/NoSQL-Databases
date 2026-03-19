#!/usr/bin/env python3
from pymongo import MongoClient, errors
import random
from datetime import datetime, timedelta
import time
import sys
import os

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

class ShardedCinemaDB:
    def __init__(self, uri = "mongodb://mongos:27017/"):
        """Подключение к роутеру mongos."""
        uri = os.getenv("MONGO_URI", "mongodb://mongos:27017/")
        self.client = wait_for_mongodb(uri)
        self.db = self.client["etl_db"]
    
    def show_shard_status(self):
        """Показывает статус шардирования."""
        print("Sharding Status")
        collections = ["movie_views", "user_payments", "content_ratings", "search_queries"]
        
        for coll_name in collections:
            coll = self.db[coll_name]
            count = coll.count_documents({})
            
            try:
                stats = self.db.command("collStats", coll_name)
                sharded = stats.get("sharded", False)
                
                if sharded:
                    print(f"\n{coll_name}: {count} docs")
                    print(f"Sharded")
                else:
                    print(f"\n{coll_name}: {count} docs")
                    print(f"Not sharded")
            except:
                print(f"\n{coll_name}: {count} docs")
    
    def search_by_user(self):
        """Поиск просмотров конкретного пользователя."""
        print("Search by User")

        pipeline = [{"$group": {"_id": "$user_id"}}, {"$limit": 5}]
        sample_users = list(self.db.movie_views.aggregate(pipeline))
        sample_users = [u["_id"] for u in sample_users if u.get("_id")]
        
        if not sample_users:
            print("No users found")
            return 0, 0
            
        print(f"Users samples: {sample_users}")
        
        user_id = input("Insert user_id (example user_1): ").strip()
        
        start_time = time.time()
        views = list(self.db.movie_views.find(
            {"user_id": user_id},
            {"_id": 0}
        ).sort("start_time", -1).limit(20))
        
        elapsed = (time.time() - start_time) * 1000
        print(f"\nUser's views {user_id} (found: {len(views)}, time: {elapsed:.2f} ms):")
        for view in views[:5]:
            print(f"{view.get('movie_title')} - {view.get('watch_duration_minutes')} min - {view.get('start_time')}")  
        
        return len(views), elapsed
    
    def search_by_movie(self):
        """Поиск всех просмотров фильма."""
        print("Search by Movie")
        
        pipeline = [{"$group": {"_id": "$movie_title"}}, {"$limit": 5}]
        sample_movies = list(self.db.movie_views.aggregate(pipeline))
        sample_movies = [m["_id"] for m in sample_movies if m.get("_id")]
        
        if not sample_movies:
            print("No movies found")
            return
            
        print(f"Movies: {sample_movies}")
        
        movie = input("Insert movie title: ").strip()
        
        start_time = time.time()
        views = list(self.db.movie_views.find(
            {"movie_title": movie},
            {"_id": 0}
        ).sort("start_time", -1).limit(20))
        
        elapsed = (time.time() - start_time) * 1000
        
        print(f"\nMovie views '{movie}': {len(views)}")
        print(f"Completion time: {elapsed:.2f} ms")
        
        if views:
            users = set(v.get('user_id') for v in views if v.get('user_id'))
            print(f"Unique users: {len(users)}")
    
    def compare_performance(self):
        """Сравнение производительности запросов."""
        print("Performance Test")
        
        print("\nSearch by user (sharded by user_id):")
        times = []
        
        pipeline = [{"$group": {"_id": "$user_id"}}, {"$limit": 5}]
        users_data = list(self.db.movie_views.aggregate(pipeline))
        users = [u["_id"] for u in users_data if u.get("_id")]
        
        if users:
            for i in range(5):
                user = random.choice(users)
                start = time.time()
                list(self.db.movie_views.find({"user_id": user}).limit(10))
                times.append((time.time() - start) * 1000)
        
        if times:
            print(f"Avg time: {sum(times)/len(times):.2f} ms")
            print(f"Min/Max: {min(times):.2f}/{max(times):.2f} ms")
        
        print("\nSearch by movie (all shards):")
        times = []
        
        pipeline = [{"$group": {"_id": "$movie_title"}}, {"$limit": 5}]
        movies_data = list(self.db.movie_views.aggregate(pipeline))
        movies = [m["_id"] for m in movies_data if m.get("_id")]
        
        if movies:
            for i in range(5):
                movie = random.choice(movies)
                start = time.time()
                list(self.db.movie_views.find({"movie_title": movie}).limit(10))
                times.append((time.time() - start) * 1000)
        
        if times:
            print(f"Avg time: {sum(times)/len(times):.2f} ms")
            print(f"Min/Max: {min(times):.2f}/{max(times):.2f} ms")
        
        print("\nWriting data:")
        times = []
        for i in range(5):
            test_view = {
                "view_id": f"perf_test_{i}_{int(time.time())}",
                "user_id": f"user_{random.randint(1, 50)}",
                "movie_id": "movie_100",
                "movie_title": "Test Movie",
                "start_time": datetime.now().isoformat() + "Z",
                "watch_duration_minutes": 90
            }
            start = time.time()
            self.db.movie_views.insert_one(test_view)
            times.append((time.time() - start) * 1000)
        
        if times:
            print(f"Avg time: {sum(times)/len(times):.2f} ms")
    
    def menu(self):
        while True:
            print("\nONLINE CINEMA - SHARDED DB")
            print("1. Sharding Status")
            print("2. Search by User")
            print("3. Search by Movie")
            print("4. Performance Test")
            print("5. Exit")
            
            choice = input("Select action: ").strip()
            if choice == "1":
                self.show_shard_status()
            elif choice == "2":
                self.search_by_user()
            elif choice == "3":
                self.search_by_movie()
            elif choice == "4":
                self.compare_performance()
            elif choice == "5":
                print("Au revoir!")
                break
            else:
                print("Wrong choice")

if __name__ == "__main__":
    uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://localhost:27000/"
    db = ShardedCinemaDB(uri)
    db.menu()