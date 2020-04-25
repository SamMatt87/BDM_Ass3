#!/usr/bin/env bash
from mrjob.job import MRJob
from mrjob.step import MRStep
typeid=1
question=6
user = 7
title=11
tag=12
class MRUsers(MRJob):
	def mapper(self, _, row):
		post=row.split(',')
		letters='''QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasddfghjklzxcvbnm '''
		def strip_punc(text):
			string=""
			for char in text:
				if char in letters:
					string=string+char
			return string

		if post[typeid] =='1':
			if ' big data ' in strip_punc(post[question]).lower():
				yield post[user],1
			elif ' big data ' in strip_punc(post[title]).lower():
				yield post[user],1
			elif ' big data ' in strip_punc(post[tag]).lower():
				yield post[user],1
	def reducer(self, key,values):
		yield '*',(key,sum(values))
	def reducer_2(self, key,values):
		sorted_list=sorted(list(values), key=lambda x:x[0])
		for k in sorted_list:
			yield k[0],k[1]
	def steps(self):
		return[
			MRStep(mapper=self.mapper,
				reducer=self.reducer),
			MRStep(reducer=self.reducer_2)]
if __name__=='__main__':
	MRUsers.run()
