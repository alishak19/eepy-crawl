We did not use any third party extensions.

We partitioned our crawls, and ran PageRank and Indexer on those partitions. We created one large compiled index table using appends, and merged all crawl tables (and all PageRank tables) using merge-script.py. These zip files, along with the entire set of crawls in another folder, are in Drive.

Prior to compiling, unzip the relevant worker folders for indexer, PageRank, and crawler, and add all tables to worker folders (partitioned already by pre-determined IDs). If running in local, add worker folders to the local repository; if in EC2, add to the repository root directory. To compile the code files on EC2, run ./crawler-ec2-script. If compiling in local, run ./script.sh. 

To run the search engine on EC2 after running the script:
sudo java -Xmx80g -cp bin cis5550.frontend.EepyCrawlSearch 80 > program.log 2>&1 &

Job-related commands (after running the script) are listed below:
Crawler: java -cp bin cis5550.flame.FlameSubmit localhost:9000 new-crawler.jar cis5550.jobs.NewCrawler
PageRank: java -cp bin cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.NewPageRank
Indexer: java -cp bin cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer
