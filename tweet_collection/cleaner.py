import re
import nltk
from nltk.corpus import stopwords
from nltk import PorterStemmer
from nltk.stem import WordNetLemmatizer

#nltk.download()
cachedStopWords = stopwords.words("english")


def remove_stop_words(text):
    text = re.sub(r'http://[\w.]+/+[\w.]+', "", text, re.IGNORECASE)
    text = re.sub(r'https://[\w.]+/+[\w.]+', "", text, re.IGNORECASE)

    text = text.rstrip("\r\n")
    text = text.rstrip("\n")
    text = text.replace("\r\n", "")
    text = text.replace("\r", "")
    text = text.replace("\n", "")
    text = text.replace("\N", "")

    text = re.sub('[^A-Za-z\s]+', ' ', text)
    text = ' '.join([word for word in text.split() if word.lower() not in cachedStopWords])
    text_cleaned = text.encode('utf-8').strip()
    return text_cleaned


def stem_word(word):
    return PorterStemmer.stem_word(word)


def stem_word_wordnet_lemmatizer(word):
    wnl = WordNetLemmatizer()
    return wnl.lemmatize(word)


def remove_stop_words_and_stem(text):
    cleaned_text = remove_stop_words(text)
    stems = set()
    for word in cleaned_text.split():
        stems.add(stem_word(word))

    return stems