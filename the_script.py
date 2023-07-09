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

#### STEP 1. CONNECT TO DATABASE AND GET LIST OF PODCASTS ####

db_file = r"D:\podcastindex_feeds.db"
transcripts_folder = r"D:\\Transcripts"

if not os.path.exists(transcripts_folder):
    os.makedirs(transcripts_folder)

conn = sqlite3.connect(db_file)
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM podcasts")
row_count = c.fetchone()[0]

def fetch_podcasts(cursor):
    cursor.execute("SELECT url, folder FROM podcasts")
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        yield row

#### STEP 2. PARSE THE LINK, ADD NEW EPISODES ####

# Loop through each row in the result
for i, (url, folder) in enumerate(fetch_podcasts(c), start=1):
    
    print(f"Now processing: {i}/{row_count} {folder}")
    
    # Make sure the folder exists
    try:
        folder_addr = os.path.join(transcripts_folder, folder)
        abs_path = os.path.abspath(folder_addr) #I made it absolute path because it broke once
        
        if not os.path.exists(abs_path):
            os.makedirs(abs_path)
    except Exception as e:
        print(f"Cannot create the folder:", e)      
        
    try:
        podcastparser.normalize_feed_url(url)

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        req = urllib.request.Request(url, headers=headers)
        response = urllib.request.urlopen(req)

        parsed = podcastparser.parse(url, response)
        if not parsed:
            os.system('cls' if os.name == 'nt' else 'clear')
            continue
    except Exception as e:
        print(f'The parser could not parse the link {url}:', e)
        os.system('cls' if os.name == 'nt' else 'clear')
        continue         
         
    # Create a new database file in the folder
    try:
        db_path = os.path.join(abs_path, "feed.db")
        conn2 = sqlite3.connect(db_path)
        c2 = conn2.cursor()
        
        # Create a table with the required fields
        c2.execute('''CREATE TABLE IF NOT EXISTS feed_data
                        (title TEXT, published TIMESTAMP, url TEXT, duration TEXT, description TEXT, guid TEXT, script BOOLEAN DEFAULT 0, audio BOOLEAN DEFAULT 0)''')
    except Exception as e:
        print(f"An error occured while creating the db file:", e)
        
    # Insert the episode
    for episode in parsed['episodes']:
        enclosures = episode.get('enclosures')
        if enclosures and len(enclosures) > 0:
            ins_url = enclosures[0].get('url')
        else:
            continue
        ins_title = episode['title']
        ins_guid = episode.get('guid')
        ins_published = episode.get('published')
        ins_duration = episode.get('total_time', 0)
        ins_description = episode.get('description')
        
        # Check if GUID exists
        c2.execute("SELECT EXISTS (SELECT 1 FROM feed_data WHERE guid = ?)", (ins_guid,))
        exists = c2.fetchone()[0]
        
        # If doesn't - insert into db file and write into missing_episodes
        if not exists:
            c2.execute("INSERT INTO feed_data (title, published, url, duration, description, guid) VALUES (?, ?, ?, ?, ?, ?)",
                  (ins_title, ins_published, ins_url, ins_duration, ins_description, ins_guid))
            
    conn2.commit()
    
    # Find out how many episodes are missing script and audio
    
    # Count rows where script is 0
    c2.execute("SELECT COUNT(*) FROM feed_data WHERE script = 0")
    missing_script = c2.fetchone()[0]
    
    # Count rows where audio is 0
    c2.execute("SELECT COUNT(*) FROM feed_data WHERE audio = 0")
    missing_audio = c2.fetchone()[0]
    
    if missing_script == 0 and missing_audio == 0:
        torch.cuda.empty_cache()
        gc.collect()
        os.system('cls' if os.name == 'nt' else 'clear')
        continue
    
    c2.execute("SELECT COUNT(*) FROM feed_data WHERE script = 0 OR audio = 0")
    missing_either = c2.fetchone()[0]
    
    # Load the db
    c2.execute('SELECT * FROM feed_data')
    rows = c2.fetchall()
    
    # Clean up title of the episode, used in the loop
    def clean_title(title):
        forbidden_symbols = r'[\u003c\u003e\u003a\u0022\u002f\u005c\u007c\u003f\u002A\u002e]'
        cleaned_title = re.sub(forbidden_symbols, '', title)
        cleaned_title = cleaned_title.replace(' ', '_')
        return cleaned_title
    
#### STEP 3. LOOP THROUGH EPISODES, EXECUTE CODE BASED ON BOOLEN VALUES ####
    
    i2 = 1 #episode number, this is jank
    # Start the loop of executing code based on boolean fields
    for row in rows:
        
        title, published, url, duration, description, guid, script, audio = row
        
        # Establish the filepath
        published_date = datetime.fromtimestamp(published).strftime('%Y%m%d')
        cleaned_title = clean_title(title)
        filename = f"{published_date}_{cleaned_title}"
        filepath = os.path.join(folder_addr, filename)
        
        if script and audio or script is None:
            continue
        
        print(f"Current episode: {i2}/{missing_either}", filename)
        i2 = i2+1
        
        MAX_RETRIES = 1
        RETRY_INTERVAL = 10
        
        # If all booleans are satisfied, skip, if not, download and convert audio
        downloaded = False
        for i in range(MAX_RETRIES):
            try:
                r = requests.get(url, timeout=60)
                with open(filepath, 'wb') as f2:
                    f2.write(r.content)
                print(f"Downloaded the episode file.")
                downloaded = True
                break
            except Exception as e:
                print("An error occurred while downloading an episode:", e)
                print(f"Retrying in {RETRY_INTERVAL} seconds...")
                time.sleep(RETRY_INTERVAL)
                continue
        if not downloaded:
            print("Download failed, skipping")
            del downloaded
            continue

        try:
            if not filepath.endswith('.wav'):
                wav_audio = AudioSegment.from_file(filepath)
                new_filepath = os.path.splitext(filepath)[0] + '.wav'
                wav_audio.export(new_filepath, format='wav')
                os.remove(filepath)
                filepath = new_filepath
                print("Converted the episode file.")
        except Exception as e:
            print("An error occured while converting the file:", e)
        
        # If the episode is not transcribed
        if script is not None and not script:
            try:    
                model = whisper.load_model("medium")
                result = model.transcribe(filepath, verbose=False, temperature=0, compression_ratio_threshold=2.4, logprob_threshold=-1.0, no_speech_threshold=0.6, condition_on_previous_text=True, initial_prompt="Transcribe the following podcast recording with precise attention to grammar, syntax, and punctuation." )
                srt_writer = get_writer("srt", folder_addr)
                srt_writer(result, filepath)
                
                print("Transcribed the episode.")
                
                # Change 'script' to 1 to mark it as transcribed
                script = True
                c2.execute('UPDATE feed_data SET script = ? WHERE guid = ?', (script, guid))
                conn2.commit()
            except Exception as e:
                print("An error occured while calling Whisper:", e)
        
        # If episode's audio is not saved        
        if audio is not None and not audio:
            try:
                episode_audio = AudioSegment.from_wav(filepath)
                
                mp3_path = os.path.splitext(filepath)[0] + '.mp3'
                bitrate = "128k"
                
                episode_audio.export(mp3_path, format="mp3", bitrate=bitrate)
                print("Saved the mp3 file.")
                
                # Change 'audio' to 1 to mark it as transcribed
                audio = True
                c2.execute('UPDATE feed_data SET audio = ? WHERE guid = ?', (audio, guid))
                conn2.commit()
            except Exception as e:
                print(f"An error occured while saving mp3 file:", e)
                
        # Delete audio file
        try:
            os.remove(filepath)
            print(f"Deleted the episode file.")
        except Exception as e:
            print("An error occurred while deleting an audio file:", e)
        
        # Garbage collection
        try:
            del model
            del result
            torch.cuda.empty_cache()
            gc.collect()
            os.system('cls' if os.name == 'nt' else 'clear')
        except Exception as e:
            print("An error occured while garbage collecting:",e)