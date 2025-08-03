# full_mlb_pipeline.py

import os
import datetime
import shutil

# Set today's date string
today = datetime.date.today().strftime('%Y-%m-%d')

# Define paths
combined_dir = "baseball/combined"
article_dir = "mlb_baseball_article_generator"

# Step 1: Run combine script to generate structured player JSON
print("🔧 Running mlb_combine_all_files.py to generate structured player file...")
os.system("python mlb_combine_all_files.py")

# Step 2: Run analyzer to enhance structured file
print("📊 Running player_stats_analyzer.py to enhance structured player file...")
os.system("python player_stats_analyzer.py")

# Step 3: Copy enhanced file into article generator folder
enhanced_file_name = f"enhanced_structured_players_{today}.json"
enhanced_file_path = os.path.join(combined_dir, enhanced_file_name)
target_path = os.path.join(article_dir, enhanced_file_name)

if os.path.exists(enhanced_file_path):
    print(f"📁 Copying {enhanced_file_name} to article generator directory...")
    shutil.copy(enhanced_file_path, target_path)
else:
    raise FileNotFoundError(f"❌ Enhanced structured file not found: {enhanced_file_path}")

# Step 4: Generate DFS article
print("📝 Generating DFS article for today...")
os.system(f"python {article_dir}/generate_dfs_article.py")

print("✅ Full MLB pipeline completed successfully.")
