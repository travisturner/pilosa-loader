package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	hash "github.com/aviddiviner/go-murmur"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
	u "github.com/travisturner/pilosa-loader/user"
)

type Main struct {
	BasePath   string
	Hosts      []string
	IndexName  string
	BufferSize uint
	Bucket     string
	Prefix     string
	AWSRegion  string
	S3svc      *s3.S3
	S3files    []*s3.Object
	totalBytes int64
	bytesLock  sync.RWMutex
	totalRecs  *Counter
	indexer    pdk.Indexer
	nexter     *pdk.Nexter
	index      *gopilosa.Index
	client     *gopilosa.Client
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		nexter:    pdk.NewNexter(),
		totalRecs: &Counter{},
	}
	return m
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func randBool(n int) bool {
	x := rand.Intn(100)
	return x < n
}

func randInt(from, to int) int {
	return from + rand.Intn(to-from)
}

var (
	leagues = []int32{10, 46, 41, 23, 28, 90, 600}
	teamCnt = []int{50, 50, 5000, 5000, 50, 50, 5000}
	buckets = []string{"High", "Medium", "Low"}
)

func randFav() u.Favorite {
	// choose a random leage, then team
	r := rand.Intn(len(leagues))
	leg := leagues[r]
	tem := int32(rand.Intn(teamCnt[r]))
	buc := buckets[rand.Intn(3)]

	return u.Favorite{
		League_id: leg,
		Sport_id:  leg,
		Team_id:   tem,
		Bucket:    buc,
	}
}

func randFavs() []u.Favorite {
	opts := []int{0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 3}
	rnd := rand.Intn(len(opts))
	favs := make([]u.Favorite, opts[rnd])
	for ii := range favs {
		favs[ii] = randFav()
	}
	return favs
}

func RandomUser() *u.User {
	typ := ""
	if randBool(10) {
		typ = "registered"
	}
	gender := "U"
	if randBool(80) {
		if randBool(50) {
			gender = "M"
		} else {
			gender = "F"
		}
	}
	return &u.User{
		Swid:   randString(8),
		Type:   typ,
		Gender: gender,
		Age:    randInt(10, 100),
		//Registered_country // NOT USED
		Registered_dma_id:      fmt.Sprintf("%d", randInt(0, 10000)),
		Registered_postal_code: fmt.Sprintf("%d", randInt(10000, 99999)),
		Is_league_manager:      randBool(5),
		Plays_fantasy:          randBool(25),
		Stated_teams_favorites: randFavs(),
		PageViews:              randInt(0, 65535),
		TimeSpent:              randInt(0, 10000000),
		//VideoStarts // NOT USED
		VideoCompletes:   randInt(0, 65535),
		Visits:           randInt(0, 65535),
		Hits:             randInt(0, 65535),
		HasFavorites:     randBool(20),
		HasNotifications: randBool(10),
		HasAutostart:     randBool(20),
		IsInsider:        randBool(20),

		//Latitude // NOT USED
		//Longitude// NOT USED
		//IsRegistered // NOT USED
		Derived_teams: randFavs(),
	}
}

func main() {
	indexName := flag.String("index", "user360", "Index name.")
	hosts := flag.String("hosts", "localhost:10101", "Pilosa server hosts as comma separated list.")
	bufSize := flag.Int("bufSize", 1000000, "Import buffer size.")
	region := flag.String("region", "us-east-1", "AWS Region.")
	hash := flag.String("hash", "", "Print hash value for a string and exit.")
	gen := flag.Bool("gen", false, "Generate Users and exit.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] <S3Bucket> <S3Prefix>\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *hash != "" {
		fmt.Printf("Hash value is %d.\n", get64BitHash(*hash))
		os.Exit(0)
	}

	if *gen {
		cnt := 10

		for ii := 0; ii < cnt; ii++ {
			u := RandomUser()
			//fmt.Printf("u: %v\n", u)
			b, err := json.Marshal(u)
			if err != nil {
				log.Fatal("json marshal")
				os.Exit(0)
			}
			fmt.Println(string(b))
		}

		os.Exit(0)
	}

	if len(flag.Args()) < 2 {
		flag.Usage()
		log.Fatal("S3 Bucket and Prefix must be specified.")
	}

	main := NewMain()
	main.Hosts = strings.Split(*hosts, ",")
	main.IndexName = *indexName
	main.BufferSize = uint(*bufSize)
	main.AWSRegion = *region

	fmt.Printf("Pilosa hosts %s.\n", main.Hosts)
	fmt.Printf("Index name %s.\n", main.IndexName)
	fmt.Printf("Buffer size %d.\n", main.BufferSize)
	fmt.Printf("AWS region %s\n", main.AWSRegion)

	if err := main.Init(); err != nil {
		log.Fatal(err)
	}

	main.Bucket = flag.Args()[0]
	main.Prefix = flag.Args()[1]

	main.LoadBucketContents()

	fmt.Printf("S3 bucket %s contains %d files for processing.\n", main.Bucket, len(main.S3files))

	ticker := main.printStats()

	s3files := make(chan *s3.Object, 100)
	users := make(chan u.User, 10000)

	go func() {
		for _, file := range main.S3files {
			s3files <- file
		}
		close(s3files)
	}()

	c := make(chan os.Signal, 1)

	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s", pdk.Bytes(main.BytesProcessed()))
			os.Exit(0)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < len(main.S3files); i++ {
		wg.Add(1)
		go func() {
			file, open := <-s3files
			if open {
				main.getUsers(file, users)
				wg.Done()
			}
		}()
	}

	var wg2 sync.WaitGroup
	//for i := 0; i < len(main.S3files); i++ {
	for i := 0; i < 5; i++ {
		wg2.Add(1)
		go func() {
			main.insertUsers(users)
			wg2.Done()
		}()
	}

	wg.Wait()
	close(users)
	wg2.Wait()

	ticker.Stop()
	time.Sleep(10 * time.Second)
	log.Printf("Completed, Last Record: %d, Bytes: %s", main.totalRecs.Get(), pdk.Bytes(main.BytesProcessed()))
	main.Close()
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

// List S3 objects from AWS bucket based on command line argument of the bucket name
func (m *Main) LoadBucketContents() {
	// Use the commented line to restrict the number of files loaded (for local testing)
	//resp, err := m.S3svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(m.Bucket), Prefix: aws.String(m.Prefix),MaxKeys: aws.Int64(10)})
	resp, err := m.S3svc.ListObjects(&s3.ListObjectsInput{Bucket: aws.String(m.Bucket), Prefix: aws.String(m.Prefix)})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", m.Bucket, err)
	}
	m.S3files = resp.Contents
}

func (m *Main) getUsers(s3object *s3.Object, users chan<- u.User) {

	result, err := m.S3svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(m.Bucket),
		Key:    aws.String(*s3object.Key),
	})
	if err != nil {
		log.Fatal(err)
	}
	scan := bufio.NewScanner(result.Body)
	buf := make([]byte, 0, 64*1024)
	scan.Buffer(buf, 1024*1024)
	i := 1
	for scan.Scan() {
		line := scan.Bytes()
		m.AddBytes(len(line))
		var u u.User
		json.Unmarshal(line, &u)
		u.RowNum = i
		users <- u
		i++
	}
	if err2 := scan.Err(); err2 != nil {
		log.Fatal(err2)
	}
}

func (m *Main) insertUsers(users <-chan u.User) {
	for user := range users {
		columnID := m.nexter.Next()
		user.ColumnID = int32(columnID)

		// Enumerated from genderMap
		genderID := uint64(u.GenderMap[user.Gender])

		if user.Age != 0 {
			m.indexer.AddValue("age_i", "age_i", columnID, int64(user.Age))
		}

		//countryID, err2 := strconv.ParseUint(u.CountryMap[user.Registered_country].Country_code, 10, 64)
		countryID := uint64(10)

		// Directly mapped
		dmaID, err3 := strconv.ParseUint(user.Registered_dma_id, 10, 64)

		// Postal code hashed to int
		postalID := get64BitHash(user.Registered_postal_code)

		m.indexer.AddBit("is_league_manager", columnID, boolToUInt64(user.Is_league_manager))
		m.indexer.AddBit("plays_fantasy", columnID, boolToUInt64(user.Plays_fantasy))
		m.indexer.AddBit("has_favorites", columnID, boolToUInt64(user.HasFavorites))
		m.indexer.AddBit("has_notifications", columnID, boolToUInt64(user.HasNotifications))
		m.indexer.AddBit("has_autostart", columnID, boolToUInt64(user.HasAutostart))
		m.indexer.AddBit("is_insider", columnID, boolToUInt64(user.IsInsider))
		m.indexer.AddBit("is_registered", columnID, boolToUInt64(user.Type == "registered"))

		// create the frames in the DB
		if genderID != 0 {
			m.indexer.AddBit("gender", columnID, genderID)
		}

		//if err2 == nil {
		m.indexer.AddBit("country", columnID, countryID)
		//}

		if err3 == nil {
			m.indexer.AddBit("dma_id", columnID, dmaID)
		}

		if postalID != 0 {
			m.indexer.AddValue("postal_code", "postal_code", columnID, postalID)
			//lat, long := u.GetLatLongFromPostalCode(user.Registered_country, user.Registered_postal_code)
			//if lat != 0 && long != 0 {
			//	latID, _ := u.MapValue("latitude", lat)
			//	m.indexer.AddValue("latitude", "latitude", columnID, latID)
			//	user.Latitude = lat
			//	longID, _ := u.MapValue("longitude", long)
			//	m.indexer.AddValue("longitude", "longitude", columnID, longID)
			//	user.Longitude = long
			//}
		}

		if user.Stated_teams_favorites != nil {
			for _, element := range user.Stated_teams_favorites {
				if _, ok := u.StatedLeagueMap[element.Sport_id]; ok {
					m.indexer.AddBit(u.StatedLeagueMap[element.Sport_id], columnID, uint64(element.Team_id))
					m.indexer.AddBit("stated_leagues", columnID, uint64(element.Sport_id))
				}
			}
		}

		if user.Derived_teams != nil {
			for _, element := range user.Derived_teams {
				switch element.Bucket {
				case "High":
					if _, ok := u.DerivedHighCCLeagueMap[element.League_id]; ok {
						m.indexer.AddBit(u.DerivedHighCCLeagueMap[element.League_id], columnID, uint64(element.Team_id))
						m.indexer.AddBit("league_cc_high", columnID, uint64(element.League_id))
					}
				case "Medium":
					if _, ok := u.DerivedMediumCCLeagueMap[element.League_id]; ok {
						m.indexer.AddBit(u.DerivedMediumCCLeagueMap[element.League_id], columnID, uint64(element.Team_id))
						m.indexer.AddBit("league_cc_medium", columnID, uint64(element.League_id))
					}
				case "Low":
					if _, ok := u.DerivedLowCCLeagueMap[element.League_id]; ok {
						m.indexer.AddBit(u.DerivedLowCCLeagueMap[element.League_id], columnID, uint64(element.Team_id))
						m.indexer.AddBit("league_cc_low", columnID, uint64(element.League_id))
					}
				}
			}
		}
		m.indexer.AddValue("swid", "swid", columnID, get64BitHash(user.Swid))

		m.indexer.AddValue("page_views", "page_views", columnID, int64(user.PageViews))
		m.indexer.AddValue("time_spent", "time_spent", columnID, int64(user.TimeSpent))
		m.indexer.AddValue("video_completes", "video_completes", columnID, int64(user.VideoCompletes))
		m.indexer.AddValue("visits", "visits", columnID, int64(user.Visits))
		m.indexer.AddValue("hits", "hits", columnID, int64(user.Hits))

		//m.client.Query(m.index.SetColumnAttrs(columnID, map[string]interface{}{"swid": user.Swid}))
		m.totalRecs.Add(1)
	}
}

func boolToUInt64(cond bool) (v uint64) {
	v = uint64(0)
	if cond {
		v = uint64(1)
	}
	return
}

func get64BitHash(s string) int64 {
	//return hash.MurmurHash64A([]byte(s), 0)
	return int64(hash.MurmurHash2([]byte(s), 0))
}

// Init function initilizations loader.
// Establishes session with Pilosa PDK and AWS S3 client
func (m *Main) Init() error {

	log.Printf("Loading GeoCode data ...")
	//u.LoadGeoCodes()

	var err error
	m.indexer, err = pdk.SetupPilosa(m.Hosts, m.IndexName, u.Frames, m.BufferSize)
	if err != nil {
		return fmt.Errorf("Error setting up Pilosa '%v'", err)
	}
	//m.client = m.indexer.Client()

	// Initialize S3 client
	sess, err2 := session.NewSession(&aws.Config{
		Region: aws.String(m.AWSRegion)},
	)

	if err2 != nil {
		return fmt.Errorf("Creating S3 session: %v", err2)
	}

	// Create S3 service client
	m.S3svc = s3.New(sess)

	return nil
}

func (m *Main) Close() {
	if err := m.indexer.Close(); err != nil {
		log.Fatal(err)
	}
}

// printStats outputs to Log current status of loader
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Bytes: %s, Records: %v, Duration: %v, Rate: %v/s, %v rec/s", pdk.Bytes(bytes), m.totalRecs.Get(), duration, pdk.Bytes(float64(bytes)/duration.Seconds()), float64(m.totalRecs.Get())/duration.Seconds())
		}
	}()
	return t
}

// Add Bytes provides thread safe processing to set the total bytes processed.
// Adds the bytes parameter to total bytes processed.
func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

// BytesProcessed provides thread safe read of total bytes processed.
func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

// Generic counter with mutex (threading) support
type Counter struct {
	num  int64
	lock sync.Mutex
}

// Add function provides thread safe addition of counter value based on input parameter.
func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

// Get function provides thread safe read of counter value.
func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}
