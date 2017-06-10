from bs4 import BeautifulSoup
from pymongo import MongoClient
import re
from datetime import datetime


def split_sighting_report(sighting_str):
    d = {}
    sighting_report = sighting_str.split('|')
    try:
        d['Occurred'] = datetime.strptime(' '.join(re.compile('^Occurred : ').split(sighting_report[0])[1].split()[:2]), "%m/%d/%Y %H:%M")
    except:
        pass
    temp = re.compile('^Reported(\w?):').split(sighting_report[1])[-1].split()
    try:
        d['Reported'] = datetime.strptime(' '.join((temp[0], temp[-1])), "%m/%d/%Y %H:%M")
    except:
        pass
    try:
        d['Posted'] = datetime.strptime(re.compile('^Posted(\w?):').split(sighting_report[2])[-1], " %m/%d/%Y")
    except:
        pass
    try:
        d['State'] = re.compile('^Location: ').split(sighting_report[3])[-1].split(',')[-1].lstrip()
    except:
        pass
    try:
        d['Shape'] = re.compile('^Shape: ').split(sighting_report[4])[-1]
    except:
        pass
    try:
        d['Duration'] = sighting_report[5].split(':')[-1]
    except:
        pass

    return d


def split_unstructured_comments(comment_str):
    d = {}
    comments = comment_str.split('|')
    d['User Comments'] = comments[0]
    nuforc = re.sub(r'\(\(NUFORC Note: ', '', comments[len(comments)-1])
    nuforc = re.sub(r'\)\)', '', nuforc)
    d['NUFORC Comments'] = nuforc.lstrip()
    return d


if __name__ == '__main__':

    # open client
    client = MongoClient()
    db = client.ufo
    coll_in = db.raw
    coll_out = db.processed

    # iterate through the raw docs and build a formatted doc
    for document in coll_in.find():

        new_doc = {}
        if ('url' in document.keys()) & ('html' in document.keys()):
            # make unique id
            url = document["url"]
            doc_id = url.split(".")[-2].split("/")[-1]
            new_doc.update({"_id": doc_id})

            # print(doc_id)

            # extract content
            bs = BeautifulSoup(document["html"], 'html.parser')
            for br in bs.find_all("br"):
                br.replace_with("|")
            tds = bs.find_all('td')
            if len(tds) == 2:
                new_doc.update(split_sighting_report(tds[0].text))
                new_doc.update(split_unstructured_comments(tds[1].text))

            # output to mongo
            coll_out.replace_one({"_id": doc_id}, new_doc, True)
        else:
            print("Fail!")
