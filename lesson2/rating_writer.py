from datetime import datetime

import argparse
import random
import time
import os 
import csv
import uuid

users=[]
movies=[]

def load_data():
    global users
    global movies
    fname = os.path.join (
        os.environ['SPARK_DATA'],'ratings.csv'
    )
    with open (fname) as csvfile:
        reader = csv.reader (csvfile, delimiter=',')
        next (reader, None)
        for row in reader:
            users.append (row[0])
            movies.append (row[1])

            
def get_rating():
    """
    Return a user:movie:rating pair
    """
    global users
    global movies
    random.seed (datetime.utcnow().microsecond)
    user = random.choice(users)
    movie = random.choice(movies)
    rating = random.choice([1,2,3,4,5])
    #print ('{},{},{}'.format(user, movie, rating))
    #return '{},{},{}\n'.format(user, movie, rating)
    print ([user, movie, rating])
    return [user, movie, rating]

def initialize (interval=0.5):
    """
    Initialize a TCP server that returns a non deterministic
    flow of simulated events to its clients
    """
    
    load_data()
    export_dir = os.path.join(os.environ['SPARK_DATA'], 'structured')
    
    if not os.path.exists (export_dir):
        os.makedirs (export_dir)
        
    print ('Writing data in {}'.format(export_dir))
    for i in range (0,5):
        rt = get_rating()
        fn = '{}.csv'.format(uuid.uuid4())
        fname = os.path.join (export_dir, fn)
        with open (fname, 'w') as f:
            writer = csv.writer(f)
            writer.writerow (rt)
#            f.write(rt)
        time.sleep (interval)
        
def main():
    if 'SPARK_DATA' not in os.environ:
        print('Error. Please define SPARK_DATA variable')
        exit(1)
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument(
        '--interval', required=False, default=0.5,
        help='Interval in seconds', type=float)
    
    args, extra_params = parser.parse_known_args()
    initialize (interval=args.interval)
    
if __name__ == '__main__':
    main()