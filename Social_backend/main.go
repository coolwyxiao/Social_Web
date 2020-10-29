package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"reflect"
	"strconv"

	"cloud.google.com/go/storage"
	jwtmiddleware "github.com/auth0/go-jwt-middleware"
	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"golang.org/x/net/context"
	elastic "gopkg.in/olivere/elastic.v3"
	//       "context"
	//       "cloud.google.com/go/bigtable"
)

const (
	INDEX    = "around"
	TYPE     = "post"
	DISTANCE = "200km"
	//PROJECT_ID = "around-811"
	//BT_INSTANCE = "around-post"
	ES_URL      = "" // change every time start vm in google cloud console
	BUCKET_NAME = "" //bucket created in google cloud to store images
//    PROJECT_ID = "around-811"
//    BT_INSTANCE = "around­-post"
)

var mySigningKey = []byte("secret")

type Location struct {
	Lat float64 `json:"lat"` //后面定义的是raw data，将大小写问题解决;名字大写是public，小写是private
	Lon float64 `json:"lon"`
}

type Post struct {
	// `json:"user"` is for the json parsing of this User field. Otherwise, by default it's 'User'.
	User     string   `json:"user"`
	Message  string   `json:"message"`
	Location Location `json:"location"`
}

func main() {
	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(INDEX).Do()
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index.
		mapping := `{
			"mappings":{
				"post":{
					"properties":{
						"location":{
							"type":"geo_point" 
						}
					}
				}
			}
		}` //将location转化成geo-point，然后直接用elastic search
		_, err := client.CreateIndex(INDEX).Body(mapping).Do()
		if err != nil {
			// Handle error
			panic(err)
		}
	}
	fmt.Println("started-service")
	// Here we are instantiating the gorilla/mux router
	r := mux.NewRouter() //Create a new router on top of the existing http router as we need to check auth.
	var jwtMiddleware = jwtmiddleware.New(jwtmiddleware.Options{
		ValidationKeyGetter: func(token *jwt.Token) (interface{}, error) {
			return mySigningKey, nil
		},
		SigningMethod: jwt.SigningMethodHS256,
	})

	r.Handle("/post", jwtMiddleware.Handler(http.HandlerFunc(handlerPost))).Methods("POST")
	r.Handle("/search", jwtMiddleware.Handler(http.HandlerFunc(handlerSearch))).Methods("GET")
	r.Handle("/login", http.HandlerFunc(loginHandler)).Methods("POST")
	r.Handle("/signup", http.HandlerFunc(signupHandler)).Methods("POST")

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8080", nil))
	// http.HandleFunc("/post", handlerPost) //endpoint和需要执行的，默认是8080端口
	// http.HandleFunc("/search",handlerSearch)
	// log.Fatal(http.ListenAndServe(":8080", nil))//出现错误之后的操作
}

//读取post请求,json请求的格式如下：
// {
// 	"user":"Lisa"
// 	"message":"Test"
// 	"location"{
// 		"lat": 37,
// 		"lon":-100
// 	}
// }
func handlerPost(w http.ResponseWriter, r *http.Request) { //传入原始值，使得更改时也可以改变
	// Parse from body of request to get a json object.
	fmt.Println("Received one post request")
	decoder := json.NewDecoder(r.Body)
	var p Post
	if err := decoder.Decode(&p); err != nil { //执行两个statement，初始化＋判断
		//这里用Decode的方式使得p中的值de reference，直接更改p的值
		panic(err)
		return
	}
	fmt.Fprintf(w, "Post received: %s\n", p.Message) //把p中的内容写入w中,并且打印下来

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")

	user := r.Context().Value("user")
	claims := user.(*jwt.Token).Claims
	username := claims.(jwt.MapClaims)["username"]

	// 32 << 20 is the maxMemory param for ParseMultipartForm,equals to 32MB
	// After you call ParseMultipartForm, the file will be saved in the server memory with maxMemory size.
	// If file size is larger than maxMemory, the rest of the data will be saved in a system temporary file.
	r.ParseMultipartForm(32 << 20)

	// Parse from form data.
	fmt.Printf("Received one post request %s\n", r.FormValue("message"))
	lat, _ := strconv.ParseFloat(r.FormValue("lat"), 64)
	lon, _ := strconv.ParseFloat(r.FormValue("lon"), 64)
	p = Post{
		User:    username.(string),
		Message: r.FormValue("message"),
		Location: Location{
			Lat: lat,
			Lon: lon,
		},
	}
	id := uuid.New() // 创造一个unique string

	file, _, err := r.FormFile("image")
	if err != nil {
		http.Error(w, "Image is not available", http.StatusInternalServerError)
		fmt.Printf("Image is not available %v.\n", err)
		return
	}
	defer file.Close()

	// ctx := context.Background()

	// replace it with real bucket name.(save to google cloud)
	// _, attrs, err := saveToGCS(ctx, file, BUCKET_NAME, id)
	// if err != nil {
	// 	http.Error(w, "GCS is not setup", http.StatusInternalServerError)
	// 	fmt.Printf("GCS is not setup %v\n", err)
	// 	return
	// }

	// Update the media link after saving to GCS.
	//p.Url = attrs.MediaLink

	// Save to ES.
	saveToES(&p, id)

	// Save to BigTable. Need to create BigTable
	//saveToBigTable(p, id)
	//       ctx := context.Background()
	//        // you must update project name here
	//        bt_client, err := bigtable.NewClient(ctx, around-811, BT_INSTANCE)
	//        if err != nil {
	//        panic(err)
	//        return
	//        }
	//        tbl := bt_client.Open("post")
	//        mut := bigtable.NewMutation()
	//        t := bigtable.Now()
	//        mut.Set("post", "user", t, []byte(p.User))
	//        mut.Set("post", "message", t, []byte(p.Message))
	//        mut.Set("location", "lat", t, []byte(strconv.FormatFloat(p.Location.Lat, 'f', ­1, 64)))
	//        mut.Set("location", "lon", t, []byte(strconv.FormatFloat(p.Location.Lon, 'f', ­1, 64)))
	//        err = tbl.Apply(ctx, id, mut) if err != nil {
	//        panic(err)
	//        return }
	//        fmt.Printf("Post is saved to BigTable: %s\n", p.Message)
	//        }

}

func saveToGCS(ctx context.Context, r io.Reader, bucketName, name string) (*storage.ObjectHandle, *storage.ObjectAttrs, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer client.Close()

	bucket := client.Bucket(bucketName)
	// Next check if the bucket exists
	if _, err = bucket.Attrs(ctx); err != nil {
		return nil, nil, err
	}

	obj := bucket.Object(name)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, r); err != nil {
		return nil, nil, err
	}
	if err := w.Close(); err != nil {
		return nil, nil, err
	}

	if err := obj.ACL().Set(ctx, storage.AllUsers, storage.RoleReader); err != nil {
		return nil, nil, err
	}

	attrs, err := obj.Attrs(ctx)
	fmt.Printf("Post is saved to GCS: %s\n", attrs.MediaLink)
	return obj, attrs, err
}

// Save a post to ElasticSearch
func saveToES(p *Post, id string) {
	// Create a client
	es_client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
		return
	}

	// Save it to index
	_, err = es_client.Index().
		Index(INDEX).
		Type(TYPE).
		Id(id).
		BodyJson(p).
		Refresh(true).
		Do()
	if err != nil {
		panic(err)
		return
	}

	fmt.Printf("Post is saved to Index: %s\n", p.Message)
}

func handlerSearch(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received one request for search")
	lat, _ := strconv.ParseFloat(r.URL.Query().Get("lat"), 64) //将String convert成为float，两个返回值还有error值
	lon, _ := strconv.ParseFloat(r.URL.Query().Get("lon"), 64)
	ran := DISTANCE
	if val := r.URL.Query().Get("range"); val != "" {
		ran = val + "km"
	}

	fmt.Printf("Search received: %f %f %s\n", lat, lon, ran)

	// Create a client
	client, err := elastic.NewClient(elastic.SetURL(ES_URL), elastic.SetSniff(false)) //elasticsearch是一个api，可以远程操作这个服务
	//false表示回调函数
	if err != nil {
		panic(err)
		return
	}

	// Define geo distance query as specified in
	// https://www.elastic.co/guide/en/elasticsearch/reference/5.2/query-dsl-geo-distance-query.html
	q := elastic.NewGeoDistanceQuery("location") //起名字是location
	q = q.Distance(ran).Lat(lat).Lon(lon)

	// Some delay may range from seconds to minutes. So if you don't get enough results. Try it later.
	searchResult, err := client.Search().
		Index(INDEX).
		Query(q).
		Pretty(true). //设置函数参数
		Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d post\n", searchResult.TotalHits())

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var typ Post
	var ps []Post
	for _, item := range searchResult.Each(reflect.TypeOf(typ)) { // instance of，只挑出是post类型的返回值
		p := item.(Post) // p = (Post) item，将返回结果中强制转化成post
		fmt.Printf("Post by %s: %s at lat %v and lon %v\n", p.User, p.Message, p.Location.Lat, p.Location.Lon)

		ps = append(ps, p)

	}
	js, err := json.Marshal(ps)
	if err != nil {
		panic(err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write(js)
	// // Return a fake post，创造一个该地区的fake post
	// p := &Post{
	// User:"1111",
	// Message:"一生必去的100个地方",
	// Location: Location{
	//        Lat:lat,
	//        Lon:lon,
	//    },
	// }
	// js, err := json.Marshal(p) //把json object转化成string
	// if err != nil {
	//        panic(err)
	//        return
	// }

	// w.Header().Set("Content-Type", "application/json") //返回的数据类型是JSON
	// w.Write(js)
}
