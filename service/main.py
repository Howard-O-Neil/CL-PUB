import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Just load 1 time per script
# Dont worry :)))
import loader
from loader import spark, trino_cursor, origins, \
    user_id, user_name, user_content_vect, user_ranking, user_org_rank, user_content_vect_float, \
    np_user_content, np_user_content_med, np_ranking, np_org_rank, \
    content_scaler, ranking_scaler, org_rank_scaler, np_features, \
    ranker, index, indexing_scale, \
    cal_collab_ranking

import numpy as np
import tensorflow as tf
from scipy.spatial import distance as sci_distance

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    res_rows = spark.sql("""
        select _id from coauthor
        limit 100
    """).collect()

    res = [r["_id"] for r in res_rows]
    
    return {"arr": res}

@app.get("/search_author")
async def search_author(name: str):
    low_name = name.lower()

    trino_cursor.execute(f"""
        SELECT author_id, author_name
        FROM author_feature
        WHERE LOWER(author_name) LIKE '%{low_name}%'
        LIMIT 200
    """)
    rows = trino_cursor.fetchall()

    return {
        "result": [{
            "author_id": r[0],
            "author_name": r[1]
        } for r in rows]
    }

top_k_ann = 5000
top_k_recommend = 50

@app.get("/get_info")
async def get_info(ids: str):
    res = []
    for id in ids.split(","):
        idx = user_id.index(id)

        user_feature_content = user_content_vect_float[idx]
        user_feature_rank = user_ranking[idx]
        user_feature_org_rank = user_org_rank[idx]
        
        np_user_feature_content = np.expand_dims(
            np.array(user_feature_content).astype(np.float32), axis=0)
        np_user_feature_content_med = np.expand_dims(np.median(np_user_feature_content, axis=1), axis=1)
        np_user_feature_rank = np.expand_dims(np.array([user_feature_rank]).astype(np.float32), axis=1)
        np_user_feature_org_rank = np.expand_dims(
            np.array([user_feature_org_rank]).astype(np.float32), axis=1)

        res.append({
            "user_id": user_id[idx],
            "name": user_name[idx],
            "content": np.multiply(content_scaler.transform(np_user_feature_content_med), indexing_scale[0]).tolist(),
            "ranking": np.multiply(ranking_scaler.transform(np_user_feature_rank), indexing_scale[1]).tolist(),
            "org_rank": np.multiply(org_rank_scaler.transform(np_user_feature_org_rank), indexing_scale[2]).tolist(),
        })

    return {
        "result": res
    }

# Tin Huynh Id: 53f47e76dabfaec09f299f95
@app.get("/recommend")
async def recommend_author(id: str):
    trino_cursor.execute(f"""
        SELECT author_id, author_name, feature, ranking, org_rank
        FROM author_feature
        WHERE author_id = '{id}'
    """)

    user_row = trino_cursor.fetchall()[0]

    user_feature_content = list(map(lambda x: float(x), user_row[2].split(";")))
    user_feature_rank = user_row[3]
    user_feature_org_rank = user_row[4]

    np_user_feature_content = np.expand_dims(
        np.array(user_feature_content).astype(np.float32), axis=0)
    np_user_feature_content_med = np.expand_dims(np.median(np_user_feature_content, axis=1), axis=1)
    np_user_feature_rank = np.expand_dims(np.array([user_feature_rank]).astype(np.float32), axis=1)
    np_user_feature_org_rank = np.expand_dims(
        np.array([user_feature_org_rank]).astype(np.float32), axis=1)

    np_user_feature = np.hstack((
            np.hstack((
                np.multiply(content_scaler.transform(np_user_feature_content_med), indexing_scale[0]), 
                np.multiply(ranking_scaler.transform(np_user_feature_rank), indexing_scale[1]))), 
            np.multiply(org_rank_scaler.transform(np_user_feature_org_rank), indexing_scale[2]))).astype(np.float32)

    distances, idxs = index.search(np_user_feature, top_k_ann)

    if id not in user_id:
        query_idx = -1    
    else: query_idx = user_id.index(id)

    squeezed_idxs = np.squeeze(idxs, axis=0)

    sorted_idx, collab_rank = cal_collab_ranking([user_feature_content, user_feature_rank, user_feature_org_rank], \
        squeezed_idxs[squeezed_idxs != query_idx])
    
    lst_sorted_idx = sorted_idx[0:top_k_recommend].tolist()
    lst_collab_rank = collab_rank[0:top_k_recommend].tolist()
    
    rec_author = []

    for _i, i in enumerate(lst_sorted_idx):
        rec_author.append({
            "author_id": user_id[i],
            "author_name": user_name[i],
            "author_rank": lst_collab_rank[_i]
        })

    return {
        "result": rec_author
    }

@app.get("/search_org")
async def search_org(name: str):
    trino_cursor.execute(f"""
        SELECT name
        FROM organization
        WHERE LOWER(name) LIKE '%{name.lower()}%'
        LIMIT 200
    """)
    rows = trino_cursor.fetchall()

    return {
        "result": rows
    }

class PairOrgPost(BaseModel):
    org1: str
    org2: str

@app.post("/recommend_org")
async def recommend_org(payload: PairOrgPost):

    trino_cursor.execute(f"""
        SELECT DISTINCT author_id
        FROM published_history
        WHERE author_org = '{payload.org1}'
    """)

    _list_id_org1 = trino_cursor.fetchall()
    list_id_org1 = [x[0] for x in _list_id_org1]

    trino_cursor.execute(f"""
        SELECT DISTINCT author_id
        FROM published_history
        WHERE author_org = '{payload.org2}'
    """)

    _list_id_org2 = trino_cursor.fetchall()
    list_id_org2 = [x[0] for x in _list_id_org2]

    filtered_id2 = list(filter(lambda x: x not in list_id_org1, list_id_org2))

    list_pairs      = []
    list_cos_dis    = []
    list_ranking    = []
    list_org_rank   = []

    for id1 in list_id_org1:
        for id2 in filtered_id2:
            u1_idx = user_id.index(id1)
            u2_idx = user_id.index(id2)

            list_pairs.append([u1_idx, u2_idx])

            u1_content_vect = user_content_vect_float[u1_idx]
            u2_content_vect = user_content_vect_float[u2_idx]

            u1_ranking = user_ranking[u1_idx]
            u2_ranking = user_ranking[u2_idx]

            u1_org_rank = user_org_rank[u1_idx]
            u2_org_rank = user_org_rank[u2_idx]

            cos_dis = np.float32(sci_distance.cosine(u1_content_vect, u2_content_vect))\
                .astype(float).item()
            ranking_prox = abs(u1_ranking - u2_ranking) \
                / max(abs(u1_ranking), abs(u2_ranking))
            org_rank_prox = 1.
            if u1_org_rank > 0 or u2_org_rank > 0:
                org_rank_prox = abs(u1_org_rank - u2_org_rank) \
                    / max(abs(u1_org_rank), abs(u2_org_rank))
            
            list_cos_dis.append(cos_dis)
            list_ranking.append(ranking_prox)
            list_org_rank.append(org_rank_prox)

    np_pairs = np.array(list_pairs)
    np_vect_prox = np.expand_dims(np.array(list_cos_dis).astype(np.float32), axis=1)
    np_ranking_prox = np.expand_dims(np.array(list_ranking).astype(np.float32), axis=1)
    np_org_rank_prox = np.expand_dims(np.array(list_org_rank).astype(np.float32), axis=1)

    predict_feature = np.hstack((np.hstack((np_vect_prox, np_ranking_prox)), np_org_rank_prox))\
        .astype(np.float32)

    collab_rank = tf.reduce_mean(ranker(predict_feature), 1).numpy()

    idx_sort = np.flip(np.argsort(collab_rank.flatten()), 0)

    pairs_idx = np_pairs[idx_sort].tolist()
    lst_collab_rank = collab_rank[idx_sort].tolist()

    return {
        "result": [
            {
                "org_1": payload.org1,
                "author_id_1": user_id[x[0]],
                "author_name_1": user_name[x[0]],
                "org_2": payload.org2,
                "author_id_2": user_id[x[1]],
                "author_name_2": user_name[x[1]],
                "rank": lst_collab_rank[i]
            } for i, x in enumerate(pairs_idx)
        ]
    }
