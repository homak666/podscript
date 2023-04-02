import gc
import json
import os
import re
import shutil
import sqlite3
import subprocess
import time
import urllib.request
import wave
from datetime import datetime

import podcastparser
import requests
import torch
import whisper
from pydub import AudioSegment
from whisper.utils import get_writer

# function for cleaning up episode name

def clean_title(title):
    forbidden_symbols = r'[\u003c\u003e\u003a\u0022\u002f\u005c\u007c\u003f\*]'
    cleaned_title = re.sub(forbidden_symbols, '', title)
    cleaned_title = cleaned_title.replace(' ', '_')
    return cleaned_title

#### STEP 1. CONNECT TO DATABASE ####

db_file = r"D:\\podcastindex_feeds.db"
transcripts_folder = r"D:\\Transcripts"

if not os.path.exists(transcripts_folder):
    os.makedirs(transcripts_folder)

conn = sqlite3.connect(db_file)
c = conn.cursor()

#### STEP 2. GET THE NEW EPISODES IN AUDIO FORM ####

# Select the URL and folder from the database
c.execute("SELECT url, folder FROM podcasts")
result = c.fetchall()

# Loop through each row in the result
for row in result:
    url = row[0]
    folder = row[1]
    
    # Make sure the folder exist and has the json in it
    try:
        folder_addr = os.path.join(transcripts_folder, folder)
        abs_path = os.path.abspath(folder_addr) #I made it absolute path because it broke once
        
        if not os.path.exists(abs_path):
            os.makedirs(abs_path)
        
        open(os.path.join(abs_path, "feed.json"), "a")
        print(f"Created folder for {folder}.")
    except Exception as e:
        print(f"Cannot create the folder or file for {folder}:", e)
    
    # Parse the json to get a list of existing episodes with download links
    #TODO: make it into function, I literally copypaste this stuff twice

    # Open the file
    try:
        json_name = os.path.join(folder_addr, "feed.json")
        print(json_name)
        with open(json_name, 'r', encoding='utf-8') as file:
            content = file.read()
        if not content.strip(): #if empty
            existing_episodes = []
        else:
    # Parse the file
            data = json.loads(content)
            entries = data.get('episodes', [])
            existing_episodes = []
            for episode in entries:
                entry = {}
                entry['title'] = episode['title']
                entry['url'] = episode['enclosures'][0]['url']
                entry['published'] = episode['published']
                existing_episodes.append(entry)
    except Exception as e:
        print(f'Could not open or parse the file {json_name}', e)
    
    # Give parser the link, get feed output in feed.json 
    
    try:
        podcastparser.normalize_feed_url(url)

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        req = urllib.request.Request(url, headers=headers)
        response = urllib.request.urlopen(req)

        parsed = podcastparser.parse(url, response)
        output_file = json_name

        with open(json_name, 'w', encoding='utf-8') as f:
            json.dump(parsed, f, ensure_ascii=False, indent=4)
    except Exception as e:
        print(f'The parser could not parse the link {url}:', e)
        continue
    
    # Parse the json to get a list of episodes with download links
    
    # Open the file
    try:
        with open(json_name, 'r', encoding='utf-8') as file:
            content = file.read()
    except Exception as e:
        print(f'Could not open the file {json_name}', e)
    
    # Parse the file
    try:
        data = json.loads(content)
        entries = data.get('episodes', [])
        episodes = []
        for entry in entries:
            episode = {}
            episode['title'] = entry['title']
            episode['url'] = entry['enclosures'][0]['url']
            episode['published'] = entry['published']
            episodes.append(episode)
    except Exception as e:
        print(f'Could not parse the file {json_name}', e)

    # Compare the contents of feed.txt file with the episode list we gathered
    missing_episodes = []
    for episode in episodes:
        if episode not in existing_episodes:
            missing_episodes.append(episode)

    # Download missing episodes and add them to feed.txt file
    for episode in missing_episodes:
        try:
            r = requests.get(episode['url'])
            published_date = datetime.fromtimestamp(episode['published']).strftime('%Y%m%d')
            cleaned_title = clean_title(episode['title'])
            filename = f"{published_date}_{cleaned_title}"
            filepath = os.path.join(folder_addr, filename)
            with open(filepath, 'wb') as f2:
                f2.write(r.content)
            print(f"Downloaded {filename} for {folder}.")
        except Exception as e:
            print("An error occurred while downloading an episode:", e)
            
#### STEP 3. CONVERT AUDIO TO WAV (JUST IN CASE), THEN TO TEXT, DELETE AUDIO ####

        # Convert audio file to wav if it's not already in wav format
        try:
            if not filepath.endswith('.wav'):
                audio = AudioSegment.from_file(filepath)
                new_filepath = os.path.splitext(filepath)[0] + '.wav'
                audio.export(new_filepath, format='wav')
                os.remove(filepath)
                filepath = new_filepath
                print(filepath)
        except Exception as e:
            print ("An error converting the file:", e)
        
        try:    
            model = whisper.load_model("medium")
            result = model.transcribe(filepath, verbose=False, temperature=0, compression_ratio_threshold=2.4, logprob_threshold=-1.0, no_speech_threshold=0.6, condition_on_previous_text=True, initial_prompt="Good day, AI. Please transcribe the following podcast recording with precise attention to grammar, syntax, and punctuation." )
            srt_writer = get_writer("srt", folder_addr)
            srt_writer(result, filepath)
        except Exception as e:
            print("An error occured while calling Whisper:", e)

        # Delete audio file
        try:
            os.remove(filepath)
            print(f"Deleted {filename} for {folder}.")
        except Exception as e:
            print("An error occurred while deleting an audio file:", e)
        
        # Garbage collection, not sure if needed but better safe then sorry
        # TODO: figure out how garbage collection even works
        del model
        del result
        torch.cuda.empty_cache()
        gc.collect()