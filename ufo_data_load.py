from pymongo import MongoClient
import csv


def write_dicts_to_csv(data, filename):
    with open(filename, 'w') as f:
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        for datum in data:
            w.writerow(datum)


if __name__ == '__main__':
    # open client
    client = MongoClient()
    db = client.ufo
    coll_in = db.processed

    documents = []
    for document in coll_in.find():
        new_doc = {k: document.get(k, None) for k in ('_id', 'State', 'User Comments')}
        documents.append(new_doc)

    write_dicts_to_csv(documents, 'ufo_data.csv')
