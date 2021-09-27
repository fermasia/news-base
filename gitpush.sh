cd /home/ec2-user/news-base
source ~/miniconda3/etc/profile.d/conda.sh
conda activate base
python /home/ec2-user/news-base/rss_pull.py
git add .
git commit -m "Commited Automatically"
git push
