# hist_box_game.property
import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.backends.backend_pdf import PdfPages
matplotlib.use('Agg')
from sklearn.preprocessing import PowerTransformer
from phik import phik_matrix
import seaborn as sns
import json
from csv_upload import get_clickhouse_client

# -----------------------------
# Config
# DB_PATH = "/home/seva/Downloads/basketball/nba.sqlite"
# DB_PATH = "/home/wsievolod/projects/basketball/nba.sqlite"

FIGS_DIR = "figs"
CORR_THRESHOLD = 0.7
# -----------------------------


def load_data():
    client = get_clickhouse_client("DATABASE_UNITED")
    query = """
    SELECT
        g.game_id,
        g.season_id,
        g.season_type,
        g.team_id_home,
        g.team_name_home,
        g.team_id_away,
        g.team_name_away,
        g.fg_pct_home,
        g.fg3_pct_home,
        g.ft_pct_home,
        g.ast_home,
        g.reb_home,
        g.stl_home,
        g.blk_home,
        g.tov_home,
        g.pf_home,
        g.fg_pct_away,
        g.fg3_pct_away,
        g.ft_pct_away,
        g.ast_away,
        g.reb_away,
        g.stl_away,
        g.blk_away,
        g.tov_away,
        g.pf_away,
        g.pts_home,
        g.pts_away
    FROM game g
    WHERE g.season_type = 'Regular Season'
    """

    result = client.query(query)
    df = pd.DataFrame(result.result_set, columns=result.column_names)
    return df


def preprocess_data(df):
    manual_exclude = ['game_id', 'season_id']
    auto_exclude = [col for col in df.columns if 'id' in col.lower()]
    columns_to_exclude = set(manual_exclude + auto_exclude)
    df_numeric = df.drop(columns=columns_to_exclude)
    for col in df_numeric.columns:
        df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')
    df_numeric = df_numeric.dropna(axis=1, how='all')
    return df_numeric.dropna(), columns_to_exclude


def plot_histograms_and_boxplots(df_clean, df_transformed, output_dir):
    for col in df_clean.columns:
        raw = df_clean[col]
        transformed = df_transformed[col]

        # Histogram
        plt.figure(figsize=(8, 4))
        plt.hist(raw, bins=30, alpha=0.6, label='Raw', edgecolor='black')
        plt.hist(transformed, bins=30, alpha=0.6, label='Transformed', edgecolor='black')
        plt.title(f'Histogram (Raw vs. Yeo-Johnson) of {col}')
        plt.xlabel(col)
        plt.ylabel('Frequency')
        plt.legend()
        plt.tight_layout()
        plt.savefig(f'{output_dir}/hist_compare_{col}.png')
        plt.close()

        # Boxplot
        plt.figure(figsize=(8, 4))
        plt.boxplot([raw, transformed], tick_labels=['Raw', 'Transformed'])
        plt.title(f'Boxplot (Raw vs. Yeo-Johnson) of {col}')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/box_compare_{col}.png')
        plt.close()

    print("✅ All comparison plots saved in ./figs")


def compute_phik_matrix(df, interval_cols):
    df_filtered = df.loc[:, df.nunique() > 1]
    return df_filtered.phik_matrix(interval_cols=interval_cols)


def save_phik_heatmap(corr_matrix, title, filename):
    plt.figure(figsize=(16, 12))
    sns.heatmap(corr_matrix, annot=False, cmap='coolwarm', square=True, cbar=True)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"📈 {title} saved to {filename}")


def save_high_correlations(corr_matrix, threshold, output_path):
    high_corr_pairs = []
    cols = corr_matrix.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            col1 = cols[i]
            col2 = cols[j]
            corr_value = corr_matrix.iloc[i, j]
            # Skip NaNs and ensure it's a number
            if pd.notna(corr_value) and corr_value >= threshold:
                high_corr_pairs.append({
                    "column_1": col1,
                    "column_2": col2,
                    "correlation": round(float(corr_value), 4)
                })

    with open(output_path, "w") as f:
        json.dump(high_corr_pairs, f, indent=4)

    print(f"🧩 Saved {len(high_corr_pairs)} high-correlation pairs to {output_path}")


def save_all_figures_to_pdf(figs_dir, output_pdf_path):
    pdf = PdfPages(output_pdf_path)
    for filename in sorted(os.listdir(figs_dir)):
        if filename.endswith(".png"):
            img_path = os.path.join(figs_dir, filename)
            img = plt.imread(img_path)
            fig, ax = plt.subplots(figsize=(11.7, 8.3))  # A4 landscape size
            ax.imshow(img)
            ax.axis('off')
            pdf.savefig(fig)
            plt.close(fig)
    pdf.close()
    print(f"📄 All figures saved into one PDF: {output_pdf_path}")


# -----------------------------
# Main Execution
# -----------------------------
df = load_data()
df_clean, columns_to_exclude = preprocess_data(df)

# Apply Yeo-Johnson
pt = PowerTransformer(method='yeo-johnson')
df_transformed = pd.DataFrame(pt.fit_transform(df_clean), columns=df_clean.columns)

# Plots
plot_histograms_and_boxplots(df_clean, df_transformed, FIGS_DIR)

# PhiK Before
df_for_corr_before = df.drop(columns=columns_to_exclude).dropna()
numeric_cols_before = df_for_corr_before.select_dtypes(include=['number']).columns.tolist()
phik_corr_before = compute_phik_matrix(df_for_corr_before, numeric_cols_before)
save_phik_heatmap(phik_corr_before, "PhiK Correlation Heatmap (Before Yeo-Johnson)", f"{FIGS_DIR}/phik_correlation_heatmap_before.png")

# PhiK After
df_corr_after = df_transformed.loc[:, df_transformed.nunique() > 1]
phik_corr_after = compute_phik_matrix(df_corr_after, df_corr_after.columns.tolist())
save_phik_heatmap(phik_corr_after, "PhiK Correlation Heatmap (After Yeo-Johnson)", f"{FIGS_DIR}/phik_correlation_heatmap_after.png")

# Save high correlations
save_high_correlations(phik_corr_after, CORR_THRESHOLD, f"{FIGS_DIR}/high_phik_correlations.json")

# Save all figures to PDF
save_all_figures_to_pdf(FIGS_DIR, f"{FIGS_DIR}/all_figures.pdf")

def plot_seaborn_with_hue(df_raw, df_transformed, hue_cols, output_dir, max_cols=5):
    os.makedirs(output_dir, exist_ok=True)
    numeric_cols = df_transformed.columns.tolist()
    
    for hue_col in hue_cols:
        if hue_col not in df_raw.columns:
            print(f"⚠️ Skipping hue '{hue_col}' — not in raw data")
            continue

        if df_raw[hue_col].nunique() > 10:
            print(f"⚠️ Skipping hue '{hue_col}' — too many unique values ({df_raw[hue_col].nunique()})")
            continue

        for col in numeric_cols[:max_cols]:  # Limit to a few features for clarity
            plt.figure(figsize=(10, 6))
            sns.violinplot(x=df_raw[hue_col], y=df_transformed[col], inner="quartile")
            plt.title(f'{col} distribution by {hue_col} (Yeo-Johnson transformed)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            filename = f"{output_dir}/violin_{col}_by_{hue_col}.png"
            plt.savefig(filename)
            plt.close()
            print(f"🎨 Saved violin plot: {filename}")

# Seaborn hue plots
categorical_hue_columns = ['season_type', 'team_name_home', 'team_name_away']
plot_seaborn_with_hue(df, df_transformed, categorical_hue_columns, FIGS_DIR)
