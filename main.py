import requests
import os
import pickle
import pandas as pd
from zipfile import ZipFile
import matplotlib as plt

class F1Stats:
    zipfile = None
    cachefile = None
    dfs = None

    def __init__(self, zipdir='f1db_csv', zipfile='f1db_csv.zip', cachefile='f1stats.pcl'):
        self.zipfile = os.path.join(zipdir, zipfile)
        self.cachefile = cachefile
    def download_data(self):
        url = 'http://ergast.com/downloads/f1db_csv.zip'
        r = requests.get(url, allow_redirects=True)
        open(self.zipfile, 'wb').write(r.content)
        # Open the zipfile
        zip_file = ZipFile(self.zipfile)
        # and read all files into a dictionary
        # key is filename (without .csv extension)
        self.dfs = {text_file.filename[:-4]: \
                        pd.read_csv(zip_file.open(text_file.filename)).replace('\\N', '') \
                    for text_file in zip_file.infolist() if text_file.filename.endswith('.csv')}
        print(self.dfs.keys())

    def save_data(self):
        with open(self.cachefile, 'wb') as handle:
            pickle.dump(self.dfs, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def load_data(self):
        with open(self.cachefile, 'rb') as handle:
            self.dfs = pickle.load(handle)

    def initialize(self, download=True):
        if download:
            self.download_data()
            self.cleanup_data()
            self.save_data()
        else:
            self.load_data()

    def __column_strip_string(self, dfname, columnnames):
        for c in columnnames:
            self.dfs[dfname][c] = self.dfs[dfname][c].str.strip()

    def __column_to_int(self, dfname, columnnames, default=0):
        for c in columnnames:
            self.dfs[dfname][c] = self.dfs[dfname][c].replace('', default).astype(int)

    def __column_to_float(self, dfname, columnnames, default=0):
        for c in columnnames:
            self.dfs[dfname][c] = self.dfs[dfname][c].replace('', default).astype(float)

    def __column_to_datetime(self, dfname, datecolumn, timecolumn=None,
                             newcolumn=None, onerror='ignore'):
        dropcolumns = []
        if timecolumn:
            self.dfs[dfname][datecolumn] = self.dfs[dfname][datecolumn] + \
                                           ' ' + self.dfs[dfname][timecolumn]
            dropcolumns.extend([timecolumn])
        targetclmn = datecolumn
        if newcolumn:
            targetclmn = newcolumn
            dropcolumns.extend([datecolumn])
        self.dfs[dfname][targetclmn] = pd.to_datetime(self.dfs[dfname][datecolumn],
                                                      errors=onerror)
        self.__drop_columns(dfname, dropcolumns)

    def __column_to_time(self, dfname, datecolumn, timecolumn=None,
                         newcolumn=None, onerror='ignore'):
        self.__column_to_datetime(dfname, datecolumn, timecolumn, newcolumn,
                                  onerror)
        targetclmn = datecolumn
        if newcolumn:
            targetclmn = newcolumn
        self.dfs[dfname][targetclmn] = self.dfs[dfname][targetclmn].dt.time

    def __column_to_date(self, dfname, datecolumn, timecolumn=None,
                         newcolumn=None, onerror='ignore'):
        self.__column_to_datetime(dfname, datecolumn, timecolumn, newcolumn,
                                  onerror)
        targetclmn = datecolumn
        if newcolumn:
            targetclmn = newcolumn
        self.dfs[dfname][targetclmn] = self.dfs[dfname][targetclmn].dt.date

    def __drop_columns(self, dfname, dropcolumns):
        self.dfs[dfname] = self.dfs[dfname].drop(columns=dropcolumns)

    def cleanup_data(self):
        # Races
        self.__column_strip_string("races", ["name"])
        self.__column_to_datetime("races", "date", "time")
        self.__column_to_datetime("races", "fp1_date", "fp1_time", "fp1")
        self.__column_to_datetime("races", "fp2_date", "fp2_time", "fp2")
        self.__column_to_datetime("races", "fp3_date", "fp3_time", "fp3")
        self.__column_to_datetime("races", "quali_date", "quali_time", "quali")
        self.__column_to_datetime("races", "sprint_date", "sprint_time", "sprint")
        self.__drop_columns("races", ["url"])
        # Results
        self.__column_to_int("results", ['position', 'number', 'fastestLap', 'rank'])
        self.__column_to_float("results", ['fastestLapSpeed'])
        self.__column_strip_string("results", ["positionText"])
        self.__column_to_time("results", "fastestLapTime")
        # Drivers
        self.__column_to_int("drivers", ['number'])
        self.__column_strip_string("drivers", ['driverRef', 'code', 'forename', 'surname', 'nationality'])
        self.__column_to_date("drivers", "dob")
        self.dfs['drivers']['name'] = self.dfs['drivers']['forename'] + ' ' + self.dfs['drivers']['surname']
        self.__drop_columns('drivers', ['url'])
        # Constructors
        self.__column_strip_string("constructors", ['constructorRef', 'name', 'nationality'])
        self.__drop_columns("constructors", ['url'])
        # Qualifying
        self.__column_to_time("qualifying", "q1")
        self.__column_to_time("qualifying", "q2")
        self.__column_to_time("qualifying", "q3")
        # Sprint results
        self.__column_to_int("sprint_results", ['position', 'number', 'fastestLap'])
        self.__column_strip_string("sprint_results", ["positionText"])
        self.__column_to_time("sprint_results", "fastestLapTime")
        # Driver standings
        self.__column_strip_string("driver_standings", ['positionText'])
        # Constructor standings
        self.__column_strip_string("constructor_standings", ['positionText'])
        # Lap times
        self.__column_to_time("lap_times", "time", onerror='coerce')
        # Pit stops
        self.__column_to_time("pit_stops", "time", onerror='coerce')

    def get_race_results(self):
        raceresults = pd.merge(self.dfs['results'][['resultId', 'raceId', 'driverId', 'constructorId',
                                                    'grid', 'position', 'positionText', 'points']],
                               self.dfs['races'][['raceId', 'year', 'round', 'name', 'date']]. \
                               rename(columns={'name': 'race'}))
        raceresults = pd.merge(raceresults,
                               self.dfs['drivers'][['driverId', 'number', 'name', 'code']]. \
                               rename(columns={'name': 'driver'}))
        raceresults = pd.merge(raceresults,
                               self.dfs['constructors'][['constructorId', 'name']]. \
                               rename(columns={'name': 'constructor'}))
        raceresults = raceresults.drop(columns=['resultId'])
        raceresults = raceresults.sort_values(['year', 'round', 'position'])
        return raceresults.copy()


def most_wins(len=25, order=False):
    winners = stats.get_race_results()
    winners = winners[winners.position == 1]
    wins = winners[['driver', 'race']].groupby(['driver']).count()
    wins = wins.sort_values('race', ascending=order)[0:len]
    return wins


def most_races(len=25, order=False):
    races = stats.get_race_results()

    races = races.where(races.position == 1)
    race = races[["race", 'raceId']].groupby(['race']).count()
    race = race.sort_values('raceId', ascending=order)[0:len]
    race = race.rename(columns={'raceId': 'num'})
    return race


stats = F1Stats()
stats.initialize(download=False)
races = most_races()
print(races)
