import duckdb
import pandas as pd
import numpy as np
from datetime import datetime


conn = duckdb.connect('./../../statsbomb_ligamx.db')
#conn = duckdb.connect('./../Dashboard/todo_plataforma/statsbomb_ligamx.db')

comps_ids = ['71', '73']


def create_team_match_stats(player_match_stats, team_catalog):
    '''
    Dados player_match_stats, los agrega a nivel equipo. 
    '''
    
    cols_2_mean = set(player_match_stats.filter(regex='_ratio|_avg|_per_').columns)
    cols_2_del = set(player_match_stats.filter(regex='_360').columns)
    cols_2_agg = set(player_match_stats.columns) - cols_2_mean - cols_2_del - set(['season_id', 'competition_id', 'home_score', 'away_score'])
    
    team_match_mean = (player_match_stats[list(cols_2_mean) + ['team_id', 'match_id']]
                       .groupby(by=['team_id', 'match_id'])
                       .mean()
                       .reset_index()
                      )
    team_match_sum = (player_match_stats[list(cols_2_agg)]
                      .groupby(by=['team_id', 'match_id'])
                      .sum()
                      .reset_index()
                     )
    team_match_stats = team_match_sum.merge(team_match_mean, on=['team_id', 'match_id'])
    
    team_id_cols = (player_match_stats[['team_id','match_id','competition_id', 'competition_name', 'season_id',
                                       'season_name','home_team','away_team','home_score','away_score','match_date',
                                        'match_info', 'match_date_str'
                                       ]]
                    .drop_duplicates()
                   )
    
    team_match_stats = team_id_cols.merge(team_match_stats, on=['team_id', 'match_id'])
    
    return team_catalog.merge(team_match_stats, on='team_id', how='outer')




def play_season_stats_date(player_match_stats, team_catalog):
    '''
    '''
    
    cols_2_mean = set(player_match_stats.filter(regex='_ratio|_avg|_per_').columns)
    cols_2_del = set(player_match_stats.filter(regex='_360').columns)
    cols_2_agg = set(player_match_stats.columns) - cols_2_mean - cols_2_del - set(['season_id', 'competition_id', 'home_score', 'away_score'])
    
    team_match_mean = (player_match_stats[list(cols_2_mean) + ['player_id', 'competition_id', 'season_id']]
                       .groupby(by=['player_id', 'competition_id', 'season_id'])
                       .mean()
                       .reset_index()
                      )
    
    print(player_match_stats.shape)
    
    team_match_sum = (player_match_stats[list(cols_2_agg) + ['competition_id', 'season_id']]
                      .groupby(by=['player_id', 'competition_id', 'season_id'])
                      .sum()
                      .reset_index()
                     )
    team_match_stats = team_match_sum.merge(team_match_mean, on=['player_id', 'competition_id', 'season_id'])
    
    #team_id_cols = (player_match_stats[['player_id','competition_id', 'competition_name', 'season_id',
    #                                   'season_name','home_team','away_team','home_score','away_score','match_date',
    #                                    'match_info', 'match_date_str'
    #                                   ]]
    #                .drop_duplicates()
    #               )
    
    team_id_cols = (player_match_stats[['player_id','competition_id', 'competition_name', 'season_id',
                                       'season_name'
                                       ]]
                    .drop_duplicates()
                   )
    
    print(team_match_stats.shape)
    team_match_stats = team_id_cols.merge(team_match_stats, on=['player_id', 'season_id', 'competition_id'])
    print(team_match_stats.shape)
    return team_catalog.merge(team_match_stats, on='team_id')

##########################################


def load_player_data(comps_ids):
    '''
    '''

    comps_ids = "'"+"','".join(comps_ids)+"'"
    
    player_match_stats = conn.sql(f"""
                                  select * from PLAYER_MATCH_STATS as pms
                                  left join MATCHES_INFO on MATCHES_INFO.match_id = pms.match_id
                                  left join PLAYER_CATALOG on PLAYER_CATALOG.player_id = pms.player_id
                                  where competition_id in ({comps_ids})
                                  """).df().drop(columns='player_id_1') #error en el 2do join
    
    player_match_stats['match_date_str'] = player_match_stats['match_date'].dt.strftime('%b %d')
    
    player_match_stats['home_score'] = player_match_stats['home_score'].fillna(-1).astype(int)
    player_match_stats['away_score'] = player_match_stats['away_score'].fillna(-1).astype(int) 
    
    player_match_stats['match_info'] = (player_match_stats['home_team']+ ' vs ' + player_match_stats['away_team'] + '<br>' +
                                    player_match_stats['home_score'].astype(str) + ':' + player_match_stats['away_score'].astype(str))
    
    team_catalog = conn.sql('select * from TEAM_CATALOG').df()
    
    return player_match_stats, team_catalog

#filtrar partidos

def filter_data(player_match_stats, beg_date, end_date):
    '''
    '''
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    beg_date = datetime.strptime(beg_date, '%Y-%m-%d')
    
    player_match_stats_filt = player_match_stats[(player_match_stats['match_date'] > beg_date) &
                                                 (player_match_stats['match_date'] < end_date)
                                                 ] 
    return player_match_stats_filt


player_match_stats, team_catalog = load_player_data(comps_ids)
player_match_stats_filt = filter_data(player_match_stats, '2024-02-01', '2022-02-01')

#cenerar agregados
play_season_aggregates = play_season_stats_date(player_match_stats_filt, team_catalog)

##########################################


