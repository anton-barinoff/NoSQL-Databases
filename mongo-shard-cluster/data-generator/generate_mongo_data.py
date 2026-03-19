#!/usr/bin/env python3

from datetime import datetime, timedelta
import random
from pymongo import MongoClient, errors
import time
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

uri = os.getenv("MONGO_URI", "mongodb://mongos:27017/")
client = wait_for_mongodb(uri)
db = client["etl_db"]

client.admin.command("enableSharding", "etl_db")

db.movie_views.drop()
db.user_payments.drop()
db.content_ratings.drop()
db.search_queries.drop()

print("Connected to MongoDB. Generating data.")

users = [f"user_{i}" for i in range(1, 101)]
movies = [
    {"id": "movie_101", "title": "Inception", "genre": "Sci-Fi"},
    {"id": "movie_102", "title": "The Dark Knight", "genre": "Action"},
    {"id": "movie_103", "title": "Interstellar", "genre": "Sci-Fi"},
    {"id": "movie_104", "title": "Pulp Fiction", "genre": "Drama"},
    {"id": "movie_105", "title": "The Matrix", "genre": "Sci-Fi"},
    {"id": "movie_106", "title": "Forrest Gump", "genre": "Drama"},
    {"id": "movie_107", "title": "The Godfather", "genre": "Crime"},
    {"id": "movie_108", "title": "The Lord of the Rings: The Return of the King", "genre": "Action"},
    {"id": "movie_109", "title": "Fight Club", "genre": "Drama"},
    {"id": "movie_110", "title": "Star Wars: Episode V - The Empire Strikes Back", "genre": "Sci-Fi"}
]

devices = [
    {"type": "smart_tv", "os": "webos", "browser": None},
    {"type": "mobile", "os": "android", "browser": "chrome"},
    {"type": "mobile", "os": "ios", "browser": "safari"},
    {"type": "desktop", "os": "windows", "browser": "firefox"},
    {"type": "tablet", "os": "ipados", "browser": "safari"},
]

# 1. Просмотры (views)
db.create_collection("movie_views")
client.admin.command("shardCollection", "etl_db.movie_views", key={"user_id": "hashed"})

views = []
for i in range(1, 1001):
    user_id = random.choice(users)
    movie = random.choice(movies)
    start_date = datetime(2025, 2, random.randint(1, 20), 
                         random.randint(0, 23), random.randint(0, 59))
    duration = random.choice([60, 90, 120, 135, 150])
    end_date = start_date + timedelta(minutes=duration)
    
    interactions = []
    if random.random() > 0.3:
        for _ in range(random.randint(0, 3)):
            pause_time = start_date + timedelta(minutes=random.randint(10, duration-10))
            interactions.append({"type": "pause", "timestamp": pause_time.isoformat() + "Z"})
            interactions.append({"type": "resume", "timestamp": (pause_time + timedelta(minutes=1)).isoformat() + "Z"})
    
    if random.random() > 0.7:
        like_time = start_date + timedelta(minutes=random.randint(20, duration-10))
        interactions.append({"type": "like", "timestamp": like_time.isoformat() + "Z"})
    
    views.append({
        "view_id": f"view_{1000+i}",
        "user_id": user_id,
        "movie_id": movie["id"],
        "movie_title": movie["title"],
        "genre": movie["genre"],
        "start_time": start_date.isoformat() + "Z",
        "end_time": end_date.isoformat() + "Z",
        "watch_duration_minutes": duration,
        "completed": random.choice([True, False, True, True]),
        "device": random.choice(devices),
        "interactions": interactions
    })

db.movie_views.insert_many(views)
print(f"Generated {len(views)} movie views")

# 2. Платежи (payments)
payments = []
for i in range(1, 601):
    user_id = random.choice(users)
    payment_date = datetime(2025, 1, random.randint(1, 31), 
                           random.randint(8, 22), random.randint(0, 59))
    
    plans = [
        {"name": "basic_monthly", "amount": 4.99},
        {"name": "premium_monthly", "amount": 9.99},
        {"name": "annual", "amount": 89.99}
    ]
    plan = random.choice(plans)
    
    payments.append({
        "payment_id": f"pay_{1000+i}",
        "user_id": user_id,
        "payment_date": payment_date.isoformat() + "Z",
        "amount": plan["amount"],
        "currency": "USD",
        "payment_method": random.choice(["credit_card", "paypal", "gift_card"]),
        "subscription_plan": plan["name"],
        "status": random.choice(["completed", "completed", "completed", "pending"]),
        "next_billing_date": (payment_date + timedelta(days=30)).isoformat() + "Z" if "monthly" in plan["name"] else (payment_date + timedelta(days=365)).isoformat() + "Z",
        "promo_code_applied": random.choice(["", "", "", "WELCOME20", "FEBRUARY2024"])
    })

db.user_payments.insert_many(payments)
print(f"Generated {len(payments)} payments")

# 3. Оценки (ratings)
db.create_collection("content_ratings")
client.admin.command("shardCollection", "etl_db.content_ratings", key={"movie_id": "hashed"})

ratings = []
for i in range(1, 801):
    user_id = random.choice(users)
    movie = random.choice(movies)
    
    ratings.append({
        "rating_id": f"rating_{1000+i}",
        "user_id": user_id,
        "movie_id": movie["id"],
        "movie_title": movie["title"],
        "rating": random.randint(3, 5),
        "review_text": random.choice([
            "Amazing movie!",
            "Good but too long",
            "Masterpiece!",
            "Worth watching",
            "",
            "Not my taste",
            "Excellent cinematography"
        ]),
        "created_at": datetime(2025, 2, random.randint(1, 20), 
                               random.randint(10, 23), random.randint(0, 59)).isoformat() + "Z",
        "helpful_count": random.randint(0, 50),
        "reports_count": random.randint(0, 2),
        "moderation_flag": random.choice([True, False]) if random.random() > 0.9 else False
    })

db.content_ratings.insert_many(ratings)
print(f"Generated {len(ratings)} ratings")

# 4. Поисковые запросы (searches)
db.create_collection("search_queries")
client.admin.command("shardCollection", "etl_db.search_queries", key={"user_id": "hashed"})

searches = []
for i in range(1, 5001):
    user_id = random.choice(users)
    search_time = datetime(2025, 2, random.randint(1, 20), 
                          random.randint(0, 23), random.randint(0, 59))
    
    queries = [
        "christopher nolan",
        "leonardo dicaprio",
        "action movies",
        "best sci-fi",
        "comedies 2024",
        "marvel",
        "oscar winners",
        "new releases"
    ]
    
    filters = {}
    if random.random() > 0.7:
        filters = {
            "genre": random.choice(["Action", "Comedy", "Drama", "Sci-Fi"]),
            "year_from": random.choice([2000, 2010, 2020]),
            "rating_min": random.choice([7, 8])
        }
    
    searches.append({
        "search_id": f"search_{1000+i}",
        "user_id": user_id,
        "query": random.choice(queries),
        "timestamp": search_time.isoformat() + "Z",
        "filters_applied": filters,
        "results_count": random.randint(5, 50),
        "clicked_movie_id": random.choice(movies)["id"] if random.random() > 0.4 else None,
        "session_id": f"sess_{random.randint(100, 999)}"
    })

db.search_queries.insert_many(searches)
print(f"Generated {len(searches)} search queries")

# Индексы для шардинга
db.movie_views.create_index([("user_id", 1)])
db.search_queries.create_index([("user_id", 1)])
db.content_ratings.create_index([("movie_id", 1)])

# Составные индексы для запросов
db.movie_views.create_index([
    ("user_id", 1), 
    ("start_time", -1)
])

db.content_ratings.create_index([
    ("movie_id", 1), 
    ("created_at", -1)
])

print(f"Collections in 'etl_db':")
print(f"- movie_views: {db.movie_views.count_documents({})} documents")
print(f"- user_payments: {db.user_payments.count_documents({})} documents")
print(f"- content_ratings: {db.content_ratings.count_documents({})} documents")
print(f"- search_queries: {db.search_queries.count_documents({})} documents")