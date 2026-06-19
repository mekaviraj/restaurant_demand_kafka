# rag/vector_db.py
import os
import math
import re

class VectorDB:
    def __init__(self, sops_dir="rag/sops"):
        self.sops_dir = sops_dir
        self.documents = {}
        self.vocab = set()
        self.idf = {}
        self.doc_vectors = {}
        self.load_documents()
        self.build_index()

    def load_documents(self):
        # Adjust base directory if necessary
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        target_dir = os.path.join(base_dir, "rag", "sops")
        if os.path.exists(target_dir):
            self.sops_dir = target_dir
            
        if not os.path.exists(self.sops_dir):
            print(f"⚠️ SOP directory not found at: {self.sops_dir}")
            return
            
        for filename in os.listdir(self.sops_dir):
            if filename.endswith(".txt"):
                path = os.path.join(self.sops_dir, filename)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        content = f.read()
                        doc_name = os.path.splitext(filename)[0]
                        self.documents[doc_name] = {
                            "name": doc_name,
                            "filename": filename,
                            "content": content,
                            "path": path
                        }
                except Exception as e:
                    print(f"⚠️ Error reading {filename}: {e}")

    def tokenize(self, text):
        return re.findall(r'[a-z0-9]+', text.lower())

    def build_index(self):
        if not self.documents:
            return

        # 1. Count term frequencies
        doc_tfs = {}
        for name, doc in self.documents.items():
            tokens = self.tokenize(doc["content"])
            tf = {}
            for t in tokens:
                tf[t] = tf.get(t, 0) + 1
            doc_tfs[name] = tf
            for t in tf.keys():
                self.vocab.add(t)

        # 2. Compute Smoothed IDF
        num_docs = len(self.documents)
        for t in self.vocab:
            count = sum(1 for tf in doc_tfs.values() if t in tf)
            self.idf[t] = math.log((1 + num_docs) / (1 + count)) + 1

        # 3. Compute normalized TF-IDF vectors
        for name, tf in doc_tfs.items():
            vector = {}
            length_squared = 0
            for t, count in tf.items():
                tfidf = count * self.idf[t]
                vector[t] = tfidf
                length_squared += tfidf ** 2
            
            length = math.sqrt(length_squared)
            if length > 0:
                for t in vector:
                    vector[t] /= length
            self.doc_vectors[name] = vector

    def search(self, query, top_k=1):
        if not self.doc_vectors:
            return []

        query_tokens = self.tokenize(query)
        query_tf = {}
        for t in query_tokens:
            if t in self.vocab:
                query_tf[t] = query_tf.get(t, 0) + 1

        query_vector = {}
        length_squared = 0
        for t, count in query_tf.items():
            tfidf = count * self.idf[t]
            query_vector[t] = tfidf
            length_squared += tfidf ** 2

        query_length = math.sqrt(length_squared)
        if query_length > 0:
            for t in query_vector:
                query_vector[t] /= query_length
        else:
            return [{"doc_name": name, "score": 0.0, "content": doc["content"]} for name, doc in list(self.documents.items())[:top_k]]

        results = []
        for name, doc_vec in self.doc_vectors.items():
            score = sum(val * doc_vec[t] for t, val in query_vector.items() if t in doc_vec)
            results.append({
                "doc_name": name,
                "score": score,
                "content": self.documents[name]["content"]
            })

        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]
