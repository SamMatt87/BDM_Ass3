from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import numpy as np

class Users(MRJob):
	def mapper(self, _, row):
		items = row.strip().split(',')	
		file_name = os.path.split(os.environ['mapreduce_map_input_file'])[-1]
		if file_name == 'Users.csv':
			ID='user:'+items[0]
			Name = 'name:'+items[3]
			yield ID,Name
		elif file_name == 'Posts.csv':
			if items[1] == '1':
				ID = 'user:'+items[7]
				post = 'post:'+items[0]
				top_post = 'toppost:'+items[0]
				subpost='subpost:'+items[0]
				yield ID,post
				yield subpost,top_post
				
			elif items[1]=='2':
				ID='user:'+items[7]
				post='post:'+items[16]
				top_post='toppost:'+items[16]
				subpost='subpost:'+items[0]
				yield ID,post
				yield subpost,top_post
		elif file_name == 'Votes.csv':
			if items[2] == '5':
				ID='user:'+items[4]
				subpost='subpost:'+items[1]
				yield subpost,ID
		elif file_name == 'Comments.csv':
			ID='user:'+items[5]
			subpost='subpost:'+items[1]
			yield subpost, ID
 
	def reducer(self,key,values):
		user_ids=[]
		for v in values:
			items=v.split(':')
			if items[0] == 'user':
				user_ids.append(v)
			elif items[0] == 'toppost':
				post='post:'+items[1]
			else:
				yield key,v
		for user in user_ids:
			yield user,post

			
	def reducer2(self,key,values):
		posts=[]
		for v in values:
			items=v.split(':')
			if items[0] == 'name':
				username = items[1]
			else:
				posts.append(items[1])
		for post in posts:
			yield username, post

	def reducer3(self,key,values):
		post_list = []
		for v in values:
			post_list.append(v)
		yield '*',(key,len(np.unique(post_list)))
	def reducer4(self,key,values):
		sorted_list=sorted(list(values), key=lambda x:(-x[1],x[0]))
		for v in sorted_list[:5]:
			yield v[0],v[1]
	def steps(self):
		return[
			MRStep(mapper=self.mapper,
				reducer=self.reducer),
			MRStep(reducer=self.reducer2),
			MRStep(reducer=self.reducer3),
			MRStep(reducer=self.reducer4)]
if __name__== '__main__':
	Users.run()
