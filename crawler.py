import urllib.request
from lxml import etree
import datetime as dt
import pytz
from pymongo import MongoClient, ASCENDING
import argparse


url = 'http://www.stproperty.sg/condominium-directory/page{}/size-50/sort-name-asc/box-1'

headers_req = {
	"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
	"Accept-Encoding": "deflate",
	"Accept-Language": "en-GB,en;q=0.9,en-US;q=0.8,zh-TW;q=0.7,zh;q=0.6,zh-CN;q=0.5",
	"Connection": "keep-alive",
	"Cookie": "PHPSESSID=35karqcdv112472tf2d8iml8i0; __utmc=28415102; xtvrn=$538353$; xtan=-; xtant=1; showPropertyTrackerCookie=TRUE; __utmz=28415102.1524636752.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utma=28415102.1109606456.1524621959.1524636752.1524639463.3; __utmt=1; __utmb=28415102.1.10.1524639463",
	"DNT": "1",
	"Host": "www.stproperty.sg",
	"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36"
}


def crawl(page_num):
	
	req = urllib.request.Request(url.format(page_num))

	print("Page Number: {}".format(page_num))

	response = urllib.request.urlopen(req)

	html_content = response.read().decode('utf - 8')

	tr = etree.HTML(html_content)

	common_path = "//div[@class='list-item-content']/div[@class='row']/\
		div[@class='col-xs-12']/div[@class='row']/div[@class='col-xs-8']"

	def _extract_text(d):
		ret = []
		for i in d:
			try:
				ret.append(i.text.strip())
			except AttributeError:
				ret.append('')
		return ret

	# extract title
	title = title = tr.xpath("{}/div[@class='row']/div[@class='col-xs-12']/h4/a".format(common_path))

	print("extracting title")

	titles = _extract_text(title)
	
	# extract address
	# road num as it separates from main road name
	address_road_num = tr.xpath("{}/div[@class='row'][2]/div[@class='col-xs-12']".format(common_path))

	road_nums = _extract_text(address_road_num)

	# road name

	road_name = tr.xpath("{}/div[@class='row'][2]/div[@class='col-xs-12']/a".format(common_path))

	print("extracting address")

	road_names = _extract_text(road_name)

	# put two together
	for i, road_num in enumerate(road_nums):
		if road_num != "":
			road_names[i] = road_num + ' ' + road_names[i]

	# extract postal code
	postal_codes = []
	for i in road_name:
		try:
			postal_codes.append(i.tail.strip()[-6:])
		except AttributeError:
			postal_codes.append('')
	
	# extract price
	price = tr.xpath("{}/div[@class='row'][4]/div[@class='col-xs-12']\
		/span[@class='price']".format(common_path))
	print("extracting price")

	prices = _extract_text(price)

	def _get_context(a):
		try:
			ret = a[0].text.strip()
		except IndexError:
			ret = ''
		return ret

	# Extra Info
	link = tr.xpath("{}/div[@class='row'][1]/div[@class='col-xs-12']/h4/a".format(common_path))
	extra_info = []
	for j in link:
		full_address = "http://www.stproperty.sg" + j.attrib['href']
		condo_req = urllib.request.Request(full_address, headers=headers_req)

		print("Extacting Page Num {}, condo: {}".format(page_num, j.attrib['title'][11:]))
		
		response_condo = urllib.request.urlopen(condo_req)
		tr_condo = etree.HTML(response_condo.read().decode('utf - 8'))

		common_path_condo = "//div[@id='details']/div[@class='detail-tab-content']/div[@class='row']/div[@class='col-xs-6'][2]/div[@class='row']"
		# No of Units
		units_num = tr_condo.xpath("{}[1]/div[@class='col-xs-8']/strong".format(common_path_condo))
		units_num = _get_context(units_num)

		# Tenure
		tenure = tr_condo.xpath("{}[2]/div[@class='col-xs-8']/a".format(common_path_condo))
		
		tenure = _get_context(tenure)

		# TOP Year
		top_year = tr_condo.xpath("{}[3]/div[@class='col-xs-8']/a".format(common_path_condo))
		top_year = _get_context(top_year)

		# Developer
		dev = tr_condo.xpath("{}[4]/div[@class='col-xs-8']/strong".format(common_path_condo))
		dev = _get_context(dev)

		ret = (units_num, tenure, top_year, dev)

		extra_info.append(ret)

	# org data for mongo insert
	data_all = []
	
	for m in range(0, 50):
		try:
			data = {}
			data['title'] = titles[m]
			data['address'] = road_names[m]
			data['postal_code'] = postal_codes[m]
			data['price'] = prices[m]
			data['tenure'] = extra_info[m][1]
			data['units_num'] = extra_info[m][0]
			data['top_year'] = extra_info[m][2]
			data['developer'] = extra_info[m][3]
			data['create_time'] = dt.datetime.now(tz=pytz.timezone('Asia/Singapore'))
			data_all.append(data)
		except IndexError:
			break

	return data_all


def crawl_all(page_start_num):
	client = MongoClient()
	db_name_ = 'web_crawl_raw'
	collection_name_ = 'stproperty'
	client[db_name_][collection_name_].create_index([("title", ASCENDING), ("postal_code", ASCENDING)], unique=False)
	
	for i in range(page_start_num, 60):
		data = crawl(i)
		print("inserting data from page {}".format(i))
		client[db_name_][collection_name_].insert_many(data)
	
	client.close()


if __name__ == '__main__':
	
	parser = argparse.ArgumentParser()
	parser.add_argument(
		'-p',
		'--page_start_num',
		default = 1,
		type=int,
		help='web page to start crawling'
	)
	
	args = parser.parse_args()
	
	crawl_all(args.page_start_num)






















