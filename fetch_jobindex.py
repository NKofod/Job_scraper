import requests
import re   
import json
import pandas as pd
from bs4 import BeautifulSoup as Soup
import time
import calendar
import os
import sqlite3 

tmp_data = requests.get("https://www.jobindex.dk/")
tmp_soup = Soup(tmp_data.content, "lxml")
tmp_soup = tmp_soup.find(class_="job-categories")
tmp_body = tmp_soup.find_all("a")
#print(tmp_body)
category_links = []
for link in tmp_body:
    category_links.append(link["href"])
categories = {}
for category in category_links:
    categories[category[5:]] = {}
print(categories)

for i in category_links:
    tmp_data = requests.get(f"https://www.jobindex.dk{i}")
    tmp_soup = Soup(tmp_data.content, "lxml")  
    tmp_soup = tmp_soup.find(class_="job-categories")
    tmp_body = tmp_soup.find_all("a")
    tmp_category_links = []
    for link in tmp_body:
        tmp_category_links.append(link["href"])
    for category in tmp_category_links:
        print(category)
        tmp_list = category.split("/")
        categories[tmp_list[2]][tmp_list[3]] = {}
        tmp_tmp_data = requests.get(f"https://www.jobindex.dk{category}")
        tmp_tmp_soup = Soup(tmp_tmp_data.content,"lxml")  
        tmp_tmp_soup = tmp_tmp_soup.find(class_="job-categories")
        tmp_tmp_body = tmp_tmp_soup.find_all("a")
        tmp_tmp_category_links = []
        for link in tmp_tmp_body:
            tmp_tmp_category_links.append(link["href"])
        for tmp_category in tmp_tmp_category_links:
            tmp_list = tmp_category.split("/")
            categories[tmp_list[2]][tmp_list[3]][tmp_list[4]] = {}



conn = sqlite3.connect("/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex.db")
c = conn.cursor()
with open("create_db.sql", "r") as f:
    sql_as_string = f.read()
c.executescript(sql_as_string)
tmp_id_list = list(c.execute("SELECT jobindex_id from jobs WHERE expired LIKE 'Not Applicable'"))
#print(tmp_id_list[0:10])
conn.commit()
conn.close() 

id_list = []
for i in tmp_id_list:
    id_list.append(i[0])
#print(id_list[0:10])

from datetime import datetime

full_df = pd.DataFrame(columns=["jobindex_id","title","job_url","added","company","job_description"])
job_area_df = pd.DataFrame(columns=["jobindex_id","area"])
job_category_df = pd.DataFrame(columns=["jobindex_id","category","subcategory"])
company_df = pd.DataFrame(columns=["company_name"])
title_df = pd.DataFrame(columns=["title"])
category_df = pd.DataFrame(columns=["category"])
subcategory_df = pd.DataFrame(columns=["category","subcategory"])
geo_df = pd.DataFrame(columns=["area"])
present_list = []
out_time = time.strftime('%Y_%m_%d_%H_%M',time.localtime())
for i in list(categories.keys()):
    # print(i)
    for j in list(categories[i].keys()):
        # print(j)
        for k in list(categories[i][j].keys()):
            if k == "danmark":
                continue
            out_dict = {
                "jobindex_id": [],
                "title": [],
                "job_url": [],
                "added": [],
                "expired": [],
                "company": [],
                "job_description": []}
            job_category_dict  ={
                "jobindex_id": [],
                "category": [],
                "subcategory": []
            }
            job_area_dict = {
                "jobindex_id": [],
                "area": []
            }
            company_dict = {"company_name": []}
            title_dict = {"title":[]}
            category_dict = {"category":[]}
            subcategory_dict = {"category":[],
                "subcategory": []}
            geo_dict = {"area":[]}
            
            # print(k)
            try: 
                tmp_data = requests.get(f"https://www.jobindex.dk/jobsoegning/{i}/{j}/{k}",timeout=60)
            except TimeoutError as ke:
                # print(str(ke))
                # print(time.localtime())
                time.sleep(90)
                tmp_data = requests.get(f"https://www.jobindex.dk/jobsoegning/{i}/{j}/{k}",timeout=60)
            except ConnectionError as ke:
                # print(str(ke))
                # print(time.localtime())
                time.sleep(90)
                tmp_data = requests.get(f"https://www.jobindex.dk/jobsoegning/{i}/{j}/{k}",timeout=60)
            except:
                # print("Other error")
                # print(datetime.now().strftime("%H:%M:%S")) 
                time.sleep(90)
                tmp_data = requests.get(f"https://www.jobindex.dk/jobsoegning/{i}/{j}/{k}",timeout=60)
            tmp_soup = Soup(tmp_data.content,"lxml")
            try: 
                next_page = tmp_soup.find(class_="page-item-next").find("a")["href"]
            except AttributeError:
                next_page = "Not none"
            #print(next_page)
            print(f"{i}/{j}/{k}\t\t Page 1 - {datetime.now().strftime('%H:%M:%S')}")
            tmp_soup = tmp_soup.find_all(class_="jobsearch-result")
            #print(len(tmp_soup))
            while next_page != None:
                tmp_string = next_page.split("?")[-1]
                tmp_string = tmp_string.split("=")[-1]
                print(f"{i}/{j}/{k}\t\tPage {tmp_string} -  - {datetime.now().strftime('%H:%M:%S')}")
                for job in tmp_soup:
                    try: 
                        tmp_id = list(job.children)[0]["data-beacon-tid"]
                        #print(tmp_id)

                        tid = job.find(class_="jix-toolbar__pubdate")
                        tid = tid.find("time").text.split("-")
                        tid = (int(tid[2]),int(tid[1]),int(tid[0]),0,0,0)
                        tid = calendar.timegm(tid)
                        # print(tid)
                        job = job.find(class_="PaidJob-inner")
                        
                        links = job.find_all("a")
                        link = links[1]
                        if link.text == "Fejlmeld annonce":
                            continue 
                        text = job.find_all("p")
                        subtitle = text[0].text.split("\n")[1]
                        #print(subtitle)
                        search_list = ["\u00f8","\u00e5","\u00e6","\u00c5","\u00d8","\u00c6"]
                        sub_list = ["ø","å","æ","Å","Ø","Æ"]
                        for word in range(len(search_list)):
                            # print(tmp)
                            subtitle = subtitle.replace(search_list[word],sub_list[word])

                        text = text[1:]
                        out_text = ""
                        for line in text:
                            out_text += line.text + "\n"
                        for word in range(len(search_list)):
                            # print(tmp)
                            out_text = out_text.replace(search_list[word],sub_list[word])
                        if link.text == "Fejlmeld annonce":
                            continue
                        else:
                            title = link.text
                            for word in range(len(search_list)):
                            # print(tmp)
                                title = title.replace(search_list[word],sub_list[word])
                            # categories[i][j][k][tmp_id] = {"link": link["href"],"title": title, "company": subtitle, "text": out_text}
                            if tmp_id not in id_list:
                                out_dict["jobindex_id"].append(tmp_id)
                                out_dict["title"].append(title)
                                out_dict["job_url"].append(link["href"])
                                out_dict["added"].append(tid)
                                out_dict["expired"].append("Not Applicable")
                                out_dict["company"].append(subtitle)
                                out_dict["job_description"].append(out_text)
                                job_area_dict["jobindex_id"].append(tmp_id)
                                job_area_dict["area"].append(k)
                                job_category_dict["category"].append(i)
                                job_category_dict["subcategory"].append(j)
                                job_category_dict["jobindex_id"].append(tmp_id)
                                company_dict["company_name"].append(subtitle)
                                title_dict["title"].append(title)
                                category_dict["category"].append(i)
                                subcategory_dict["category"].append(i)
                                subcategory_dict["subcategory"].append(j)
                                geo_dict["area"].append(k)
                            else:
                                present_list.append(tmp_id)
                                continue



                    except IndexError:
                        pass
                    except AttributeError:
                        pass

                if next_page != "Not none":
                    try: 
                        tmp_data = requests.get(next_page,timeout=60)
                        tmp_soup = Soup(tmp_data.content, "lxml")
                    except TimeoutError as ke:
                        #print(str(ke))
                        #print(datetime.now().strftime("%H:%M:%S"))
                        time.sleep(90)
                        tmp_data = requests.get(next_page,timeout=60)
                        tmp_soup = Soup(tmp_data.content,"lxml")
                    except ConnectionError as ke:
                        #print(str(ke))
                        #print(datetime.now().strftime("%H:%M:%S"))
                        time.sleep(90)
                        tmp_data = requests.get(next_page,timeout=60)
                        tmp_soup = Soup(tmp_data.content, "lxml")
                    except:
                        #print("Other error")
                        #print(datetime.now().strftime("%H:%M:%S"))
                        time.sleep(90)
                        tmp_data = requests.get(next_page,timeout=60)
                        tmp_soup = Soup(tmp_data.content, "lxml")
                    try: 
                        next_page = tmp_soup.find(class_="page-item-next").find("a")["href"]

                        tmp_soup = tmp_soup.find_all(class_="jobsearch-result")
                    except AttributeError:
                        next_page = None 
                else:
                    next_page = None

            tmp_full_df = pd.DataFrame.from_dict(out_dict)
            full_df = full_df.append(tmp_full_df,ignore_index=True).drop_duplicates()
            #print(full_df[["job_description","jobindex_id"]])
            tmp_company_df = pd.DataFrame.from_dict(company_dict)
            company_df = company_df.append(tmp_company_df,ignore_index=True).drop_duplicates()            
            tmp_title_df = pd.DataFrame.from_dict(title_dict)
            title_df = title_df.append(tmp_title_df,ignore_index=True).drop_duplicates()
            tmp_category_df = pd.DataFrame.from_dict(category_dict)
            category_df = category_df.append(tmp_category_df,ignore_index=True).drop_duplicates()
            tmp_subcategory_df = pd.DataFrame.from_dict(subcategory_dict)
            subcategory_df = subcategory_df.append(tmp_subcategory_df,ignore_index=True).drop_duplicates()            
            tmp_geo_df = pd.DataFrame.from_dict(geo_dict)                      
            geo_df = geo_df.append(tmp_geo_df,ignore_index=True).drop_duplicates()
            tmp_job_area_df = pd.DataFrame.from_dict(job_area_dict)
            job_area_df = job_area_df.append(tmp_job_area_df,ignore_index=True).drop_duplicates()
            tmp_job_category_df = pd.DataFrame.from_dict(job_category_dict)
            job_category_df = job_category_df.append(tmp_job_category_df,ignore_index=True).drop_duplicates()
        with open(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_log/log_jobindex_{out_time}.txt","a") as log_file:
            print(len(full_df), file = log_file)
            print(job_area_df, file = log_file)
            print(job_category_df, file = log_file)

company_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/companies_{out_time}.csv",sep=";",index=False)
title_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/title_{out_time}.csv",sep=";",index=False)
category_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/category_{out_time}.csv",sep=";",index=False)
subcategory_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/subcategory_{out_time}.csv",sep=";",index=False)
geo_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/geo_{out_time}.csv",sep=";",index=False)
full_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/full_{out_time}.csv",sep=";",index=False)
job_category_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/job_category_{out_time}.csv",sep=";",index=False)
job_area_df.to_csv(f"/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex_data/job_area_{out_time}.csv",sep=";",index=False)
expired_list = []
for i in id_list:
    if i in present_list:
        pass 
    else:
        expired_list.append(i)

conn = sqlite3.connect("/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/jobindex.db")
company_df.to_sql("companies",conn,index=False, if_exists="append")
title_df.to_sql("titles",conn,index=False, if_exists="append")
category_df.to_sql("categories",conn,index=False, if_exists="append")
subcategory_df.to_sql("subcategories",conn,index=False, if_exists="append")
geo_df.to_sql("geo_area",conn,index=False, if_exists="append")
full_df.to_sql("jobs",conn,index=False, if_exists="append")
job_category_df.to_sql("job_category",conn,index=False,if_exists="append")
job_area_df.to_sql("job_area",conn,index=False,if_exists="append")

with open("/home/kofod/Desktop/Projects/Programming/Auto_Job_Application/Jobindex_log.log","a") as logfile: 
    string = f"Ran fetch_jobindex.py at {out_time}. We have {len(full_df['jobindex_id'].to_list())} new jobs and {len(expired_list)} expired jobs."
    logfile.write(string+"\n")
c = conn.cursor()
expired_date = calendar.timegm(time.gmtime())
for i in expired_list:
    c.execute(f"UPDATE jobs SET expired={expired_date} WHERE jobindex_id = '{i}';")
conn.commit()
conn.close()