# Learning English assistant(Gmail)
### 簡介
* 原專案來源([傳送門](https://github.com/ChickenBenny/Airflow-Learning-English-tool))
* 原作品 HackMD 版本([傳送門](https://hackmd.io/-BLvXFm3STqacYMSedyKWA?view))
* 原作品 Medium 版本([傳送門](https://medium.com/@ChickenBenny/%E5%AD%B8%E7%BF%92%E8%8B%B1%E6%96%87%E5%B0%8F%E5%B7%A5%E5%85%B7%E9%96%8B%E7%99%BC%E7%AD%86%E8%A8%98-78f8a813c7af))


這是一個應用airflow系統來達到每日推撥英文學習的小工具,
此作品為改自 ChickenBenny 的作品,將其使用telegram推撥的方式改為使用Gmail來進行推撥,也透過大神的教學文章來學習Airflow這套ETL管理工具的應用及原理

### Quick start
1. Clone the repo and get into the folder
```
$ git clone https://github.com/yinghao1019/airflow-learning-english-practice.git
$ cd Airflow-Learning-English-tool
```
2. Use docker-compose up to build the app
```
$ docker-compose up -d
```
3. Set the connection and make sure you have youtube api
```
$ docker exec -it webserver airflow connections add 'database' --conn-type 'postgres' --conn-login 'airflow' --conn-password 'airflow' --conn-host 'database' --conn-port '5432' --conn-schema 'airflow'
```
4. You need to change the api token in the folders, which are `/dags/credentials` and `/init/credentials`.
![](https://i.imgur.com/6Kq09lx.png)

5.You need to set send main config in `./application.ini`
![](https://imgur.com/f2khQel.png)

5. Have fun and play with it !!

### 如何取得第三方服務
1. Youtube API 的取得方法請詳 : https://medium.com/%E5%BD%BC%E5%BE%97%E6%BD%98%E7%9A%84%E8%A9%A6%E7%85%89-%E5%8B%87%E8%80%85%E7%9A%84-100-%E9%81%93-swift-ios-app-%E8%AC%8E%E9%A1%8C/101-%E4%BD%BF%E7%94%A8-youtube-data-api-%E6%8A%93%E5%8F%96%E6%9C%89%E8%B6%A3%E7%9A%84-youtuber-%E5%BD%B1%E7%89%87-mv-d05c3a0c70aa
2. 發送Gamil 信件 的方法請詳 : https://www.learncodewithmike.com/2020/02/python-email.html

### 調整的的功能
1. 新增寄發Email的模板
2. 調整儲存影片的video table欄位(標題,簡述,發布時間,瀏覽/按讚人數)
3. 調整更新影片的流程與寄送資料的流程
4. 新增推撥的影片

### 資料庫架構
![](https://i.imgur.com/jxyOsgM.png)


### Gmail 寄發信件Demo
![Imgur](https://i.imgur.com/GECSo0E.jpg)


