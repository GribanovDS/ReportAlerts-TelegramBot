# Automated Reporting with Telegram Bot and Airflow
It's time to automate the basic reporting for our application. Let's set up an automatic sending of an analytical summary to Telegram every morning! Here's what we need to do:

Create your own Telegram bot using @BotFather
To get the chat_id, use the link https://api.telegram.org/bot<your_bot_token>/getUpdates or the bot.getUpdates() method
Write a script to generate a report on the news feed. The report should consist of two parts:
Text with information about the values of key metrics for the previous day
A graph with metric values for the previous 7 days
Display the following key metrics in the report:
- DAU
- Views
- Likes
- CTR
Automate the report sending with Airflow. Place the code for generating the report in GitLab by following these steps:
- Clone the repository
- Inside the dags folder of your local copy, create a folder with the same name as your GitLab username, which should include the @ symbol
- Create a DAG in that folder, which should be in a .py file format
- Push the results to GitLab
- Enable the DAG when it appears in Airflow
The report should be sent every day at 11:00 AM in the chat.
## Setup Instructions
Creating a Telegram Bot
- Open the Telegram app and search for the @BotFather bot.
- Send the command /newbot to @BotFather.
- Follow the prompts to create your bot and receive the bot token.
- Save the bot token for later use.
## Getting the chat_id
- Open a web browser and enter the following link, replacing <your_bot_token> with the bot token you received from @BotFather: https://api.telegram.org/bot<your_bot_token>/getUpdates
- Send a message to your bot on Telegram.
- Refresh the web page and look for the chat_id value in the JSON response.
- Save the chat_id for later use.

# Alert System for Application
This project aims to build an alert system for our application to periodically check key metrics, such as active users in the feed/messenger, views, likes, CTR, and the number of sent messages. It detects anomalies in the metrics using statistical methods.

## Features
- Periodically checks key metrics every 15 minutes
- Detects anomalies using statistical methods
- Sends alerts to a chat with information about the metric, its value, and the deviation magnitude
- Provides additional information, such as graphs and links to BI dashboards and charts to investigate the anomaly

## Examples of Bot`s work:
### Everyday report:
![image](https://user-images.githubusercontent.com/74065724/230918149-9fb0aa9d-3495-403c-b2c6-6032aaad2ed6.png)

![image](https://user-images.githubusercontent.com/74065724/230918294-2d984453-a29d-43ad-a0df-3089592e2e90.png)


### Alerts:
![image](https://user-images.githubusercontent.com/74065724/230917728-3d6f41d6-ee7f-4799-b7ef-3f019bef05a6.png)
