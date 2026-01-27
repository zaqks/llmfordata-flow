
import os
import gc
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for better memory handling
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# 1. Publications over time
def plot_publications_over_time(df, output_dir=None):
    if 'date' not in df.columns:
        return
    temp_df = df[['date']].copy()
    temp_df['date'] = pd.to_datetime(temp_df['date'], errors='coerce')
    counts = temp_df['date'].dt.to_period('M').value_counts().sort_index()
    del temp_df
    
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Month')
    ax.set_ylabel('Number of Publications')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '1.png' if output_dir is None else os.path.join(output_dir, '1.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 2. Publications by source
def plot_publications_by_source(df, output_dir=None):
    counts = df['source'].value_counts()
    ax = counts.plot(kind='bar', figsize=(8,5))
    ax.set_xlabel('Source')
    ax.set_ylabel('Number of Publications')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '2.png' if output_dir is None else os.path.join(output_dir, '2.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 3. Topic frequency distribution
def plot_topic_frequency(df, output_dir=None):
    topics = df['topics'].dropna().str.split(',')
    all_topics = [t.strip() for sublist in topics for t in sublist]
    s = pd.Series(all_topics)
    counts = s.value_counts()
    del all_topics, s
    
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Frequency')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '3.png' if output_dir is None else os.path.join(output_dir, '3.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 4. Keyword frequency (top terms)
def plot_keyword_frequency(df, top_n=20, output_dir=None):
    keywords = df['keywords'].dropna().str.split(',')
    all_keywords = [k.strip() for sublist in keywords for k in sublist]
    s = pd.Series(all_keywords)
    counts = s.value_counts().head(top_n)
    del all_keywords, s
    
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Keyword')
    ax.set_ylabel('Frequency')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '4.png' if output_dir is None else os.path.join(output_dir, '4.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 5. Topic co-occurrence heatmap
def plot_topic_cooccurrence(df, top_n=20, output_dir=None):
    topics = df['topics'].dropna().str.split(',')
    all_topics = [t.strip() for sublist in topics for t in sublist]
    top_topics = pd.Series(all_topics).value_counts().head(top_n).index
    del all_topics
    
    # Build co-occurrence matrix
    from collections import Counter
    # Build co-occurrence matrix
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
    fname = '5.png' if output_dir is None else os.path.join(output_dir, '5.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del matrix, fig, ax, im, top_topics
    gc.collect()

# 6. Emerging algorithms by topic
def plot_emerging_algorithms_by_topic(df, top_n=15, output_dir=None):
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
    del topic_algo, topic_algo_df
    
    ax = counts.plot(kind='bar', figsize=(10,5))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Unique Emerging Algorithms')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '6.png' if output_dir is None else os.path.join(output_dir, '6.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 7. Emerging algorithms trend over time
def plot_emerging_algorithms_trend(df, output_dir=None):
    temp_df = df.dropna(subset=['emerging_algorithms', 'date']).copy()
    temp_df['date'] = pd.to_datetime(temp_df['date'], errors='coerce')
    temp_df = temp_df.dropna(subset=['date'])
    temp_df['year'] = temp_df['date'].dt.year
    algos = []
    for _, row in temp_df.iterrows():
        year = row['year']
        for a in row['emerging_algorithms'].split(','):
            algos.append((year, a.strip()))
    algo_df = pd.DataFrame(algos, columns=['year', 'algorithm'])
    counts = algo_df.groupby('year')['algorithm'].nunique()
    del temp_df, algos, algo_df
    
    ax = counts.plot(marker='o', figsize=(10,5))
    ax.set_xlabel('Year')
    ax.set_ylabel('Unique Emerging Algorithms')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '7.png' if output_dir is None else os.path.join(output_dir, '7.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 8. Impact distribution
def plot_impact_distribution(df, output_dir=None):
    counts = df['impact'].value_counts()
    ax = counts.plot(kind='bar', figsize=(8,5))
    ax.set_xlabel('Impact')
    ax.set_ylabel('Count')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '8.png' if output_dir is None else os.path.join(output_dir, '8.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 9. Impact by topic
def plot_impact_by_topic(df, top_n=15, output_dir=None):
    topic_impact = []
    for _, row in df.dropna(subset=['topics', 'impact']).iterrows():
        topics = [t.strip() for t in row['topics'].split(',')]
        for t in topics:
            topic_impact.append((t, row['impact']))
    topic_impact_df = pd.DataFrame(topic_impact, columns=['topic', 'impact'])
    counts = topic_impact_df.groupby(['topic', 'impact']).size().unstack(fill_value=0)
    counts = counts.loc[counts.sum(axis=1).sort_values(ascending=False).head(top_n).index]
    del topic_impact, topic_impact_df
    
    ax = counts.plot(kind='bar', stacked=True, figsize=(12,6))
    ax.set_xlabel('Topic')
    ax.set_ylabel('Count')
    for spine in ax.spines.values():
        spine.set_visible(False)
    plt.tight_layout()
    fname = '9.png' if output_dir is None else os.path.join(output_dir, '9.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, ax
    gc.collect()

# 10. Source vs impact heatmap
def plot_source_vs_impact_heatmap(df, top_n=15, output_dir=None):
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
    fname = '10.png' if output_dir is None else os.path.join(output_dir, '10.png')
    plt.savefig(fname, dpi=100, bbox_inches='tight')
    plt.close('all')
    del counts, fig, ax, im
    gc.collect()

# Main function to generate all charts
def generate_all_charts(df, output_dir=None):
    plot_publications_over_time(df, output_dir=output_dir)
    plot_publications_by_source(df, output_dir=output_dir)
    plot_topic_frequency(df, output_dir=output_dir)
    plot_keyword_frequency(df, output_dir=output_dir)
    plot_topic_cooccurrence(df, output_dir=output_dir)
    plot_emerging_algorithms_by_topic(df, output_dir=output_dir)
    plot_emerging_algorithms_trend(df, output_dir=output_dir)
    plot_impact_distribution(df, output_dir=output_dir)
    plot_impact_by_topic(df, output_dir=output_dir)
    plot_source_vs_impact_heatmap(df, output_dir=output_dir)
    gc.collect()
