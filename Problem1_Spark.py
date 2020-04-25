from pyspark import SparkContext
import sys
class Users():
	def run(self, input_path, output_path):
		typeid=1
		question=6
		user = 7
		title=11
		tag=12
		letters='''QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasddfghjklzxcvbnm '''
		def strip_punc(text):
			string=""
			for char in text:
				if char in letters:
					string=string+char
			return string
		def find_big_data(text):
			post=text.split(',')
			if post[typeid]=='1':
				if ' big data ' in strip_punc(post[question]).lower():
					return True
				elif ' big data ' in strip_punc(post[title]).lower():
					return True
				elif ' big data ' in strip_punc(post[tag]).lower():
					return True
			return False
		sc = SparkContext('local', 'post_count')
		textfile = sc.textFile(input_path)

		posts = textfile.filter(find_big_data).map(lambda x: (int(x.split(',')[user]),1)).sortByKey()
		posts.saveAsTextFile(output_path)
		sc.stop()
if __name__=="__main__":
	users=Users()
	users.run(sys.argv[-2],sys.argv[-1])
