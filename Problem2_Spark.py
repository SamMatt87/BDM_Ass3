from pyspark import SparkContext
import sys

class Users():
	def run(self, dataset,output):
		if dataset[-1] != '/':
			dataset+='/'
		posts_path = dataset+'Posts.csv'
		users_path = dataset+'Users.csv'
		comments_path = dataset+'Comments.csv'
		votes_path = dataset+'Votes.csv'
		sc= SparkContext('local','Users')

		users_file = sc.textFile(users_path)
		posts_file = sc.textFile(posts_path)
		comments_file = sc.textFile(comments_path)
		votes_file = sc.textFile(votes_path)

		names = users_file.map(lambda x: (x.split(',')[0],x.split(',')[3]))
		questions = posts_file.filter(lambda x: x.split(',')[1]=='1').map(lambda x: (x.split(',')[7],x.split(',')[0]))
		answers = posts_file.filter(lambda x: x.split(',')[1]=='2').map(lambda x: (x.split(',')[7],x.split(',')[16]))
		def parent(x):
			x=x.split(',')
			if x[1] == '1':
				return x[0]
			else:
				return x[16]
		parents = posts_file.map(lambda x:(x.split(',')[0],parent(x)))
		comments=comments_file.map(lambda x: (x.split(',')[1],x.split(',')[5]))
		votes = votes_file.filter(lambda x: x.split(',')[2] == '5').map(lambda x: (x.split(',')[1],x.split(',')[4]))
		comments_parents = comments.join(parents).map(lambda x: (x[1][0], x[1][1]))
		vote_parents = votes.join(parents).map(lambda x: (x[1][0], x[1][1]))
		
		total = questions.union(vote_parents).union(answers).union(comments_parents)
		
		named_total = total.join(names).map(lambda x: (x[1][1],x[1][0]))
		distinct_total = named_total.distinct()
		counts=distinct_total.map(lambda user: (user[0],1)).reduceByKey(lambda a, b: a+b)
		sort=sc.parallelize(counts.sortBy(lambda x:(-x[1],x[0])).take(5))
		sort.saveAsTextFile(output)

		

if __name__=="__main__":
	users=Users()
	users.run(sys.argv[-2],sys.argv[-1])

