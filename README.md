# Telegram-Workout-Tracker
 
The Workout Tracker is a Python-based Telegram bot designed to help users log, track, and analyze their workout routines.
Users can select a workout type (Push, Pull, Legs, Cardio) and their respective exercises and repetitions. The bot also provides simple analytics with just a press of a button like the last workouts or the personal records for each exercise. 

The bot is hosted on AWS Lambda for serverless compute and utilizing the Telegram app as a UI. The data is stored in Dynamo DB tables for maximum speed and cost-efficiency. 
To provide data security the data is stored with the individual telegram chat id as partition and access key, allowing multi-tenancy for this bot and is storing highly sensitive application data on AWS Secret Manager. 
