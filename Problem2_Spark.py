from pyspark import SparkContext
import sys

class Users():
	def run(self, dataset):
		if dataset[-1] != '/':
			dataset+='/'
		posts_path = dataset+'Posts.csv'
		users_path = dataset+'Users.csv'
		comments_path = dataset+'Comments.csv'
		votes_path = dataset+'Votes.csv'
		sc= SparkContext('local','Users')
		posts_file = sc.textFile(posts_path).map(lambda x: (x.strip().split(',')[7]))
		posts_file.collect()
		

if __name__=="__main__":
	users=Users()
	users.run(sys.argv[-1])

