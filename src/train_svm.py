from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.linear_model import SGDClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

import numpy as np
import sqlite3

conn = sqlite3.connect('./data/twitter.db')
cursor = conn.cursor()

shooting_tweets = cursor.execute('select tweet from shooting').fetchall()
null_tweets = cursor.execute('select tweet from null_tweets').fetchall()
tweets = [tweet[0] for tweet in shooting_tweets]
tweets += [tweet[0] for tweet in null_tweets]

labels = ['shooting' for tweet in tweets] + ['null_tweet' for tweet in null_tweets]
X_train, X_test, y_train, y_test = train_test_split(tweets, labels)

text_clf = Pipeline([
    ('vect', CountVectorizer()),
    ('tfidf', TfidfTransformer()),
    ('clf', SGDClassifier(loss='hinge', penalty='l2', alpha=1e-3, n_iter=5, random_state=42))
])

text_clf.fit(X_train, y_train)

predicted = text_clf.predict(X_test)
print(np.mean(predicted == y_test))
