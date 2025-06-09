import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlalchemy

df_player = pd.read_csv('dataset/csv/player.csv')
df_team = pd.read_csv('dataset/csv/team.csv')

fig = plt.figure(figsize=(10, 5))
df_player['id'].plot(kind='scatter', title='Player ID')
plt.savefig('figs/player_id.png')

fig = plt.figure(figsize=(10, 5))
df_team['id'].plot(kind='scatter', title='Team ID')
plt.savefig('figs/team_id.png')