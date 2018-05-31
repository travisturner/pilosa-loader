package user

import (
	"math"

	gopilosa "github.com/pilosa/go-pilosa"
	"github.com/pilosa/pdk"
)

type User struct {
	RowNum                 int
	ColumnID               int32
	Swid                   string     `json:"user_id"`
	Type                   string     `json:"user_type"`
	Gender                 string     `json:"gender"`
	Age                    int        `json:"age"`
	Registered_country     string     `json:"registered_country"`
	Registered_dma_id      string     `json:"registered_dma_id"`
	Registered_postal_code string     `json:"registered_postal_code"`
	Is_league_manager      bool       `json:"is_league_manager"`
	Plays_fantasy          bool       `json:"plays_fantasy"`
	Stated_teams_favorites []Favorite `json:"stated_teams_favorites"`
	PageViews              int        `json:"page_views"`
	TimeSpent              int        `json:"time_spent"`
	VideoStarts            int        `json:"video_starts"`
	VideoCompletes         int        `json:"video_completes"`
	Visits                 int        `json:"visits"`
	Hits                   int        `json:"hits"`
	HasFavorites           bool       `json:"has_favorites"`
	HasNotifications       bool       `json:"has_notifications"`
	HasAutostart           bool       `json:"has_autostart"`
	IsInsider              bool       `json:"is_insider"`

	Latitude      float32
	Longitude     float32
	IsRegistered  bool
	Derived_teams []Favorite `json:"derived_team_rf"`
}

type Favorite struct {
	Team_name  string `json:"team_name"`
	Sport_name string `json:"sport_name"`
	Team_id    int32  `json:"team_id"`
	Sport_id   int32  `json:"sport_id"`
	League_id  int32  `json:"league_id"`
	Bucket     string `json:"bucket"`
}

var (
	GenderMap = map[string]int64{"M": 1, "F": 2, "U": 3}

	StatedLeagueMap          = map[int32]string{10: "stated_teams_mlb", 46: "stated_teams_nba", 41: "stated_teams_ncaab", 23: "stated_teams_cfb", 28: "stated_teams_nfl", 90: "stated_teams_nhl", 600: "stated_teams_soccer"}
	DerivedHighCCLeagueMap   = map[int32]string{10: "derived_high_cc_teams_mlb", 46: "derived_high_cc_teams_nba", 41: "derived_high_cc_teams_ncaab", 23: "derived_high_cc_teams_cfb", 28: "derived_high_cc_teams_nfl", 90: "derived_high_cc_teams_nhl", 600: "derived_high_cc_teams_soccer"}
	DerivedMediumCCLeagueMap = map[int32]string{10: "derived_medium_cc_teams_mlb", 46: "derived_medium_cc_teams_nba", 41: "derived_medium_cc_teams_ncaab", 23: "derived_medium_cc_teams_cfb", 28: "derived_medium_cc_teams_nfl", 90: "derived_medium_cc_teams_nhl", 600: "derived_medium_cc_teams_soccer"}
	DerivedLowCCLeagueMap    = map[int32]string{10: "derived_low_cc_teams_mlb", 46: "derived_low_cc_teams_nba", 41: "derived_low_cc_teams_ncaab", 23: "derived_low_cc_teams_cfb", 28: "derived_low_cc_teams_nfl", 90: "derived_low_cc_teams_nhl", 600: "derived_low_cc_teams_soccer"}

	Frames = []pdk.FrameSpec{
		pdk.NewRankedFrameSpec("gender", 100),
		pdk.NewFieldFrameSpec("age_i", 0, 200),

		pdk.NewRankedFrameSpec("country", 600),
		pdk.NewFieldFrameSpec("postal_code", 0, int(math.MaxUint32)),

		pdk.NewRankedFrameSpec("dma_id", 10000),
		pdk.NewRankedFrameSpec("is_league_manager", 100),
		pdk.NewRankedFrameSpec("plays_fantasy", 100),
		pdk.NewRankedFrameSpec("has_favorites", 100),
		pdk.NewRankedFrameSpec("has_notifications", 100),
		pdk.NewRankedFrameSpec("has_autostart", 100),
		pdk.NewRankedFrameSpec("is_insider", 100),

		pdk.NewFieldFrameSpec("swid", 0, int(math.MaxUint32)),
		pdk.NewFieldFrameSpec("page_views", 0, 65535),
		pdk.NewFieldFrameSpec("time_spent", 0, 10000000),
		pdk.NewFieldFrameSpec("video_completes", 0, 65535),
		pdk.NewFieldFrameSpec("visits", 0, 65535),
		pdk.NewFieldFrameSpec("hits", 0, 65535),

		pdk.NewFieldFrameSpec("latitude", int(math.MinInt32), int(math.MaxInt32)),
		pdk.NewFieldFrameSpec("longitude", int(math.MinInt32), int(math.MaxInt32)),
		pdk.FrameSpec{
			Name:           "is_registered",
			CacheType:      gopilosa.CacheTypeRanked,
			CacheSize:      uint(100),
			InverseEnabled: true,
		},

		//pdk.NewRankedFrameSpec("stated_teams_mlb", 50),
		pdk.FrameSpec{
			Name:           "stated_teams_mlb",
			CacheType:      gopilosa.CacheTypeRanked,
			CacheSize:      uint(50),
			InverseEnabled: true,
		},
		pdk.NewRankedFrameSpec("stated_teams_nba", 50),
		pdk.NewRankedFrameSpec("stated_teams_ncaab", 5000),
		pdk.NewRankedFrameSpec("stated_teams_cfb", 5000),
		pdk.NewRankedFrameSpec("stated_teams_nfl", 50),
		pdk.NewRankedFrameSpec("stated_teams_nhl", 50),
		pdk.NewRankedFrameSpec("stated_teams_soccer", 5000),

		pdk.NewRankedFrameSpec("derived_high_cc_teams_mlb", 50),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_nba", 50),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_ncaab", 5000),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_cfb", 5000),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_nfl", 50),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_nhl", 50),
		pdk.NewRankedFrameSpec("derived_high_cc_teams_soccer", 5000),

		pdk.NewRankedFrameSpec("derived_medium_cc_teams_mlb", 50),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_nba", 50),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_ncaab", 5000),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_cfb", 5000),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_nfl", 50),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_nhl", 50),
		pdk.NewRankedFrameSpec("derived_medium_cc_teams_soccer", 5000),

		pdk.NewRankedFrameSpec("derived_low_cc_teams_mlb", 50),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_nba", 50),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_ncaab", 5000),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_cfb", 5000),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_nfl", 50),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_nhl", 50),
		pdk.NewRankedFrameSpec("derived_low_cc_teams_soccer", 5000),

		pdk.NewRankedFrameSpec("stated_leagues", 25),
		pdk.NewRankedFrameSpec("league_cc_high", 25),
		pdk.NewRankedFrameSpec("league_cc_medium", 25),
		pdk.NewRankedFrameSpec("league_cc_low", 25),
	}
)

/*
func MapValue(name string, value interface{}) (int64, error) {
	if v, ok := value.(string); ok {
		switch name {
		case "gender":
			if l, ok := GenderMap[v]; !ok {
				return 0, fmt.Errorf("Cannot map gender for %v", v)
			} else {
				return l, nil
			}
		case "country":
			if l, ok := CountryMap[v]; !ok {
				return 0, fmt.Errorf("Cannot map country code for %v", v)
			} else {
				return strconv.ParseInt(l.Country_code, 10, 64)
			}
		case "postal_code", "swid":
			return int64(get64BitHash(v)), nil
		case "dma_id":
			return strconv.ParseInt(v, 10, 64)
		}
	}
	if v, ok := value.(int64); ok {
		switch name {
		case "latitude", "longitude":
			return int64(float32(v) * float32(math.Pow10(10))), nil
		case "age_i", "hits", "visits", "time_spent", "video_completes", "page_views":
			return int64(v), nil
		}
	}
	if v, ok := value.(float32); ok {
		switch name {
		case "latitude", "longitude":
			return int64(v * float32(math.Pow10(10))), nil
		}
	}
	if v, ok := value.(float64); ok {
		switch name {
		case "latitude", "longitude":
			return int64(v * float64(math.Pow10(10))), nil
		}
	}
	if v, ok := value.(bool); ok {
		switch name {
		case "plays_fantasy":
			fallthrough
		case "has_favorites":
			fallthrough
		case "has_notifications":
			fallthrough
		case "has_autostart":
			fallthrough
		case "is_insider":
			fallthrough
		case "is_registered":
			fallthrough
		case "is_league_manager":
			return boolToInt64(v), nil
		}
	}
	return 0, fmt.Errorf("Unknown field name '%v' or unsupported type %T.", name, value)
}

func boolToInt64(cond bool) (v int64) {
	v = int64(0)
	if cond {
		v = int64(1)
	}
	return
}

func get64BitHash(s string) uint64 {
	return uint64(hash.MurmurHash2([]byte(s), 0))
}
*/
