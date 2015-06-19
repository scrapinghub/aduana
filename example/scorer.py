class KeywordScorer(object):
    def load_keywords(fname):
        with open(fname, 'r') as kw_file:
            kw = [unicode(line.strip().lower(), "utf-8") for line in kw_file]
        return kw

    keywords = load_keywords('keywords.txt')
    max_score = float(len(keywords))

    @classmethod
    def score(cls, text):
        score = 0.0
        for kw in cls.keywords:
            if kw in text:
                score += 1.0

        score /= 20.0
        if score > 1.0:
            score = 1.0

        return score
