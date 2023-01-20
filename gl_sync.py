#!/usr/bin/python3
import argparse
import gmusicapi
import os
import json
import oauth2client
import requests
import sys
import pandas as pd
import argparse
import pdb

# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total:
        print()

LAST_FM_API='http://ws.audioscrobbler.com/2.0/'
with open('secrets.txt') as f:
    secrets = json.load(f)
    LAST_FM_API_KEY = secrets['LAST_FM_API_KEY']
    LAST_FM_SECRET_KEY = secrets['LAST_FM_SECRET_KEY']

mc = gmusicapi.clients.Mobileclient()
def init_mc(oauth_location=mc.OAUTH_FILEPATH):
    if not os.path.exists(oauth_location):
        oauth = mc.perform_oauth()
    mc.oauth_login(gmusicapi.clients.Mobileclient.FROM_MAC_ADDRESS)
    return mc


def get_song_plays(gplay_client):
    print("Getting GPlay song plays")
    counter = 0
    all_songs = {}
    for song in gplay_client.get_all_songs():
        if 'playCount' in song.keys() and song['playCount'] > 0:
            if song['artist'] not in all_songs.keys():
                all_songs[song['artist']] = {}

            if song['album'].isspace():
                song['album'] = "no_album"

            if song['album'] not in all_songs[song['artist']]:
                all_songs[song['artist']][song['album']] = {}

            all_songs[song['artist']][song['album']][song['title']] = song['playCount']

    return all_songs


def get_lastfm_token():
    data = {
        'api_key': LAST_FM_API_KEY,
        'method': 'auth.gettoken',
        'format': 'json'
    }
    token_response = requests.get(LAST_FM_API, params=data)
    if token_response.status_code != 200:
        print("Bad last.fm response " + str(token_response.status_code))
        sys.exit(1)
    return json.loads(token_response.text)['token']


def get_scrobble_counts(gplay_dict, username, num_songs):
    print("Getting Last.FM scrobble counts for user {}".format(username))
    iter_counter = 0
    scrobble_tracks = {}
    for artist in gplay_dict.keys():
        for album in gplay_dict[artist].keys():
            for track in gplay_dict[artist][album].keys():
                data = {
                    'method': 'track.getInfo',
                    'api_key': LAST_FM_API_KEY,
                    'artist': artist,
                    'track': track.strip(),
                    'format': 'json',
                    'username': username
                }
                lastfm_track_info = requests.get(LAST_FM_API, params=data)
                if lastfm_track_info.status_code == 200:
                    lastfm_track_json = json.loads(lastfm_track_info.text)
                    if 'track' in lastfm_track_json.keys() and 'userplaycount' in lastfm_track_json['track'].keys():
                        if artist not in scrobble_tracks.keys():
                            scrobble_tracks[artist] = {}

                        if album not in scrobble_tracks[artist].keys():
                            scrobble_tracks[artist][album] = {}

                        scrobble_tracks[artist][album][track] = lastfm_track_json['track']['userplaycount']
                iter_counter += 1
                printProgressBar(iter_counter, num_songs)
    return scrobble_tracks


def get_api_sig(methodsig, token):
    joined_utf8_str = ''.join(['api_key', LAST_FM_API_KEY, 'method', methodsig, 'token', token, LAST_FM_SECRET_KEY]).encode('utf-8')
    api_sig = hashlib.md5(joined_utf8_str).hexdigest()
    return api_sig


def construct_batches(scrobble_tracks, sk):
    batch_size = 50
    batch_data = {
        'api_key': LAST_FM_API_KEY,
        'sk': sk,
        'method': 'track.scrobble'
    }
    counter = 0
    for track in scrobble_tracks:
        for track_scrobble in range(track['scrobbles']):
            batch_data['timestamp[%d]' % counter] = '1581378877'
            batch_data['artist[%d]' % counter] = track['artist']
            batch_data['track[%d]' % counter] = track['track']
            track['scrobble_count']
            if counter == 49:
                yield batch_data
                counter = 0
                batch_data = {
                    'api_key': LAST_FM_API_KEY,
                    'sk': sk,
                    'method': 'track.scrobble'
                }
            counter += 1
    yield batch_data


def encode_call(parameters):
    to_encode = ''.join(entry + str(parameters[entry]) for entry in sorted(parameters))
    to_encode += LAST_FM_SECRET_KEY
    return hashlib.md5(to_encode.encode('utf-8')).hexdigest()


def write_smallfiles(big_filename, smallfile_folder, lines_per_file):
    print("Breaking large CSV into smaller CSVs")
    smallfile = None
    with open(big_filename, 'r') as bigfile:
            for lineno, line in enumerate(bigfile):
                    if lineno % lines_per_file == 0:
                            if smallfile:
                                    smallfile.close()
                            small_filename = "/".join([smallfile_folder, "small_file_{}.csv".format(lineno + lines_per_file)])
                            smallfile = open(small_filename, "w")
                    smallfile.write(line)
            if smallfile:
                    smallfile.close()

def compare_counts(gplay_plays, lastfm_plays):
    compared_dict = {}
    for artist in gplay_plays.keys():
        if artist not in compared_dict:
            compared_dict[artist] = {}
        if artist not in lastfm_plays.keys():
            compared_dict[artist] = gplay_plays[artist]
        else:
            for album in gplay_plays[artist].keys():
                if album not in lastfm_plays[artist].keys():
                    compared_dict[artist][album] = gplay_plays[artist][album]
                else:
                    for track in gplay_plays[artist][album].keys():
                        if album not in compared_dict[artist].keys():
                            compared_dict[artist][album] = {}
                        if track not in lastfm_plays[artist][album].keys():
                            compared_dict[artist][album][track] = int(gplay_plays[artist][album][track])
                        else:
                            if int(gplay_plays[artist][album][track]) > int(lastfm_plays[artist][album][track]):
                                compared_dict[artist][album][track] = int(gplay_plays[artist][album][track]) - int(lastfm_plays[artist][album][track])

    return compared_dict

def create_dataframe(compared_dict):
    print("Creating Dataframe to write CSVs")
    df = pd.DataFrame(columns=("artist", "track", "album", "timestamp", "album artist", "duration"))

    for artist in compared_dict.keys():
        for album in compared_dict[artist].keys():
            for track in compared_dict[artist][album].keys():
                for i in range(compared_dict[artist][album][track]):
                    if album == "no_album":
                        album = ""
                        album_artist = ""
                    else:
                        album_artist = artist
                    line = pd.DataFrame({"artist": artist, "track": track, "album": album, "timestamp": "", "album artist": album_artist, "duration": ""}, index=[len(df)])
                    df = df.append(line, ignore_index=True)
    return df



def dict_size(p_dict):
    to_return = 0
    for artist in p_dict.keys():
        for album in p_dict[artist].keys():
            to_return += len(p_dict[artist][album])
    return to_return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CSVs to sync Last.FM scrobbles with Google Play plays")
    parser.add_argument('--username', nargs='?', type=str, help='Last.FM username', default='Gwifleez')

    args = parser.parse_args()
    write_filepath = os.path.sep.join([os.getcwd(), 'gplay_lastfm_difference.json'])
    gplay_client = init_mc()
    gplay_dict = get_song_plays(gplay_client)

    num_songs = dict_size(gplay_dict)
    lastfm_plays = get_scrobble_counts(gplay_dict, args.username, num_songs)

    compared_dict = compare_counts(gplay_dict, lastfm_plays)
    df = create_dataframe(compared_dict)

    df.to_csv('/tmp/big_difference_file.csv', index=False)

    write_smallfiles('/tmp/big_difference_file.csv', os.getcwd(), 2500)

    print("JSON file writen to {}".format(write_filepath))
