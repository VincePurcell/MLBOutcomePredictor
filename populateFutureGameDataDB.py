import mlbgame
import pymongo
import string
import statsapi
import requests
import datahelp

# MongoDB Connections
client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['MLB_DB']
gameSummaryDB = db['GameSummaryFutureGames']
boxScoreDB = db['BoxScoreFutureGames']
gamePlayerDataDB = db['GamePlayerDataFutureGames']
playerProjectionsDB = db['PlayerProjectedStatsFutureGames']
gameSummaryDB.delete_many( { } )
boxScoreDB.delete_many( { } )
gamePlayerDataDB.delete_many( { } )
playerProjectionsDB.delete_many( { } )

start_date='08/22/2021'
end_date ='08/25/2021'
year = 2021
teams = datahelp.teams

for team_id in teams.values():
    games = statsapi.schedule(start_date=start_date, end_date=end_date, team=team_id)
    for game in games:
        try:
            if team_id == game['home_id']:

                box_score_data = statsapi.boxscore_data(game['game_id'])
                box_score_data['game_id_str'] = box_score_data.pop('gameId')
                box_score_data['game_id_num'] = str(game['game_id'])

                game_id_str = box_score_data['game_id_str']
                game_id_num = box_score_data['game_id_num']

                player_data = mlbgame.players(box_score_data['game_id_str'].replace('/','_').replace('-','_'))

                home_player_data = {}
                for player in player_data.home_players:
                    home_player_data[player.boxname] = player.__dict__
                away_player_data = {}
                for player in player_data.away_players:
                    away_player_data[player.boxname] = player.__dict__
                umpire_data = {}
                for umpire in player_data.umpires:
                    umpire_data[umpire.position] = umpire.__dict__

                player_data_dict = {
                    'game_id_num'  : game_id_num,
                    'game_id_str'  : game_id_str,
                    'home_players' : home_player_data,
                    'away_players' : away_player_data,
                    'umpires'      : umpire_data
                }

                pitching_req_str = "http://lookup-service-prod.mlb.com/json/named.proj_pecota_pitching.bam?season='"
                batting_req_str = "http://lookup-service-prod.mlb.com/json/named.proj_pecota_batting.bam?season='"
                count = 0
                home_batting_proj = {}
                for batter in box_score_data['home']['battingOrder']:
                    try:
                        request_string = batting_req_str + str(year) + "'&player_id='" + str(batter) + "'"
                        batter_projected_stats = requests.get(request_string).json()
                        home_batting_proj[str(batter)] = batter_projected_stats['proj_pecota_batting']['queryResults']['row']
                    except:
                        pass

                count = 0
                away_batting_proj = {}
                for batter in box_score_data['away']['battingOrder']:
                    try:
                        request_string = batting_req_str + str(year) + "'&player_id='" + str(batter) + "'"
                        batter_projected_stats = requests.get(request_string).json()
                        away_batting_proj[str(batter)] = batter_projected_stats['proj_pecota_batting']['queryResults']['row']
                    except:
                        pass

                pitcher_projected_stats = requests.get(pitching_req_str + str(year) + "'&player_id='" + str(box_score_data['homePitchers'][1]['personId']) + "'").json()
                home_pitcher_proj = {
                    str(box_score_data['homePitchers'][1]['personId']) : pitcher_projected_stats['proj_pecota_pitching']['queryResults']['row']
                }

                pitcher_projected_stats = requests.get(pitching_req_str + str(year) + "'&player_id='" + str(box_score_data['awayPitchers'][1]['personId']) + "'").json()
                away_pitcher_proj = {
                    str(box_score_data['awayPitchers'][1]['personId']) : pitcher_projected_stats['proj_pecota_pitching']['queryResults']['row']
                }
                
                projected_stats = {
                    'game_id_num'              : game_id_num,
                    'game_id_str'              : game_id_str,
                    'home_batting_projections' : home_batting_proj,
                    'away_batting_projections' : away_batting_proj,
                    'home_pitcher_projections' : home_pitcher_proj,
                    'away_pitcher_projections' : away_pitcher_proj
                }
                
                game['game_id_num'] = game.pop('game_id')
                game['game_id_str'] = game_id_str

                boxScoreDB.insert_one(box_score_data)
                gamePlayerDataDB.insert_one(player_data_dict)
                gameSummaryDB.insert_one(game)
                playerProjectionsDB.insert_one(projected_stats)

        except:
            pass


