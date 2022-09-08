# -*-coding:utf-8-*-


import pickle
import networkx
import matplotlib.pyplot as plt

def build_nets(data):
    graph = networkx.Graph()
    node_color = {}
    for mb_id in data:
        user_id_1 = data[mb_id]['user_id']
        sentiment = data[mb_id]['sentiment']
        node_color[user_id_1] = 'b' if float(sentiment) >= 0.5 else 'r'
        if 'retweets_info' not in data[mb_id]:
            # graph.add_node(user_id_1)
            continue
        retweet = data[mb_id]['retweets_info']['data']
        for re in retweet:
            user_id_2 = re['id']
            graph.add_edge(user_id_1,user_id_2)
            node_color[user_id_2] = 'b' if float(sentiment) >= 0.5 else 'r'
    colors = []
    for node in graph:
        colors.append(node_color[node])
    fig,ax = plt.subplots()
    plt.figure(figsize=(1,1))
    networkx.draw(graph,node_color=colors,node_size=1,width=0.01,with_labels=False,font_size=2)
    plt.savefig(fname=r'F:\python_projects\PrivateWorking\Data\WeiboWebSpider-main\data\weibos.svg')
    plt.show()




def load_data(path):
    with open(path,'rb') as f:
        data = pickle.load(f)
    return data

if __name__ == "__main__":
    twitter_path = r'F:\python_projects\PrivateWorking\Data\WeiboWebSpider-main\data\weibo_result.pkl'
    data = load_data(twitter_path)
    build_nets(data)
