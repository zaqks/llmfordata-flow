import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy.orm import Session
from ...utils._db import SessionLocal, Datasource, DatasourceAnalysis

def get_analysis_df():
    session = SessionLocal()
    # Join Datasource and DatasourceAnalysis for richer context
    query = session.query(
        Datasource.id,
        Datasource.source,
        Datasource.date,
        DatasourceAnalysis.topics,
        DatasourceAnalysis.keywords,
        DatasourceAnalysis.emerging_algorithms,
        DatasourceAnalysis.impact,
        DatasourceAnalysis.created_at
    ).join(DatasourceAnalysis, Datasource.id == DatasourceAnalysis.datasource_id)
    df = pd.DataFrame([row._asdict() for row in query.all()])
    session.close()
    return df

# 1. Publications over time
def plot_publications_over_time(df):
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        counts = df['date'].dt.to_period('M').value_counts().sort_index()
        ax = counts.plot(kind='bar', figsize=(10,5))
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of Publications')
        for spine in ax.spines.values():
            spine.set_visible(False)
        plt.tight_layout()
        plt.savefig('1.png')
        plt.close()

# 2. Publications by source
def plot_publications_by_source(df):
    counts = df['source'].value_counts()
    ax = counts.plot(kind='bar', figsize=(8,5))
    ax.set_xlabel('Source')
    ax.set_ylabel('Number of Publications')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('2.png')
    plt.close()

# 3. Topic frequency distribution
def plot_topic_frequency(df):
    topics = df['topics'].dropna().str.split(',')
    all_topics = [t.strip() for sublist in topics for t in sublist]
    s = pd.Series(all_topics)
    counts = s.value_counts()
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Frequency')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('3.png')
    plt.close()

# 4. Keyword frequency (top terms)
def plot_keyword_frequency(df, top_n=20):
    keywords = df['keywords'].dropna().str.split(',')
    all_keywords = [k.strip() for sublist in keywords for k in sublist]
    s = pd.Series(all_keywords)
    counts = s.value_counts().head(top_n)
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Keyword')
    ax.set_ylabel('Frequency')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('4.png')
    plt.close()

# 5. Topic co-occurrence heatmap
def plot_topic_cooccurrence(df, top_n=20):
    topics = df['topics'].dropna().str.split(',')
    all_topics = [t.strip() for sublist in topics for t in sublist]
    top_topics = pd.Series(all_topics).value_counts().head(top_n).index
    # Build co-occurrence matrix
    from collections import Counter
    matrix = pd.DataFrame(0, index=top_topics, columns=top_topics)
    for topic_list in topics:
        topic_list = [t.strip() for t in topic_list if t.strip() in top_topics]
        for i in range(len(topic_list)):
            for j in range(i, len(topic_list)):
                matrix.loc[topic_list[i], topic_list[j]] += 1
                if i != j:
                    matrix.loc[topic_list[j], topic_list[i]] += 1
    fig, ax = plt.subplots(figsize=(10,8))
    im = ax.imshow(matrix, cmap='Blues')
    fig.colorbar(im, ax=ax, label='Co-occurrence Count')
    ax.set_xticks(range(len(top_topics)))
    ax.set_xticklabels(top_topics, rotation=90)
    ax.set_yticks(range(len(top_topics)))
    ax.set_yticklabels(top_topics)
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('5.png')
    plt.close()

# 6. Emerging algorithms by topic
def plot_emerging_algorithms_by_topic(df, top_n=15):
    # For each topic, count unique emerging algorithms
    topic_algo = []
    for _, row in df.dropna(subset=['topics', 'emerging_algorithms']).iterrows():
        topics = [t.strip() for t in row['topics'].split(',')]
        algos = [a.strip() for a in row['emerging_algorithms'].split(',')]
        for t in topics:
            for a in algos:
                topic_algo.append((t, a))
    topic_algo_df = pd.DataFrame(topic_algo, columns=['topic', 'algorithm'])
    counts = topic_algo_df.groupby('topic')['algorithm'].nunique().sort_values(ascending=False).head(top_n)
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Unique Emerging Algorithms')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('6.png')
    plt.close()

# 7. Emerging algorithms trend over time
def plot_emerging_algorithms_trend(df):
    df = df.dropna(subset=['emerging_algorithms', 'date'])
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['year'] = df['date'].dt.year
    algos = []
    for _, row in df.iterrows():
        year = row['year']
        for a in row['emerging_algorithms'].split(','):
            algos.append((year, a.strip()))
    algo_df = pd.DataFrame(algos, columns=['year', 'algorithm'])
    counts = algo_df.groupby('year')['algorithm'].nunique()
    ax = counts.plot(marker='o', figsize=(10,5))
    ax.set_xlabel('Year')
    ax.set_ylabel('Unique Emerging Algorithms')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('7.png')
    plt.close()

# 8. Impact distribution
def plot_impact_distribution(df):
    counts = df['impact'].value_counts()
    ax = counts.plot(kind='bar', figsize=(8,5))
    ax.set_xlabel('Impact')
    ax.set_ylabel('Count')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('8.png')
    plt.close()

# 9. Impact by topic
def plot_impact_by_topic(df, top_n=15):
    topic_impact = []
    for _, row in df.dropna(subset=['topics', 'impact']).iterrows():
        topics = [t.strip() for t in row['topics'].split(',')]
        for t in topics:
            topic_impact.append((t, row['impact']))
    topic_impact_df = pd.DataFrame(topic_impact, columns=['topic', 'impact'])
    counts = topic_impact_df.groupby(['topic', 'impact']).size().unstack(fill_value=0)
    counts = counts.loc[counts.sum(axis=1).sort_values(ascending=False).head(top_n).index]
    ax = counts.plot(kind='bar', stacked=True, figsize=(12,6))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Count')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('9.png')
    plt.close()

# 10. Source vs impact heatmap
def plot_source_vs_impact_heatmap(df, top_n=15):
    counts = df.groupby(['source', 'impact']).size().unstack(fill_value=0)
    # Limit to top N sources
    counts = counts.loc[counts.sum(axis=1).sort_values(ascending=False).head(top_n).index]
    fig, ax = plt.subplots(figsize=(12,8))
    im = ax.imshow(counts, cmap='Blues', aspect='auto')
    fig.colorbar(im, ax=ax, label='Count')
    ax.set_xticks(range(len(counts.columns)))
    ax.set_xticklabels(counts.columns, rotation=45)
    ax.set_yticks(range(len(counts.index)))
    ax.set_yticklabels(counts.index)
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    plt.savefig('10.png')
    plt.close()

# Main function to generate all charts
def generate_all_charts(df=None):
    if df is None:
        df = get_analysis_df()
    plot_publications_over_time(df)
    plot_publications_by_source(df)
    plot_topic_frequency(df)
    plot_keyword_frequency(df)
    plot_topic_cooccurrence(df)
    plot_emerging_algorithms_by_topic(df)
    plot_emerging_algorithms_trend(df)
    plot_impact_distribution(df)
    plot_impact_by_topic(df)
    plot_source_vs_impact_heatmap(df)
