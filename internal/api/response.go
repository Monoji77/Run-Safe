package api

type advice struct {
	Headline string `json:"headline"`
	Advice   string `json:"advice"`
}

type pm25Block struct {
	Value           float64 `json:"value"`
	Unit            string  `json:"unit"`
	Band            string  `json:"band,omitempty"`
	ObservedAt      string  `json:"observed_at"`
	SourceUpdatedAt string  `json:"source_updated_at"`
	AgeMinutes      int     `json:"age_minutes"`
}

type regionBlock struct {
	Name        string `json:"name"`
	Method      string `json:"method"`
	Approximate bool   `json:"approximate"`
}

type uvBlock struct {
	Status               string   `json:"status"`
	Index                *float64 `json:"index,omitempty"`
	Band                 string   `json:"band,omitempty"`
	ObservedAt           string   `json:"observed_at,omitempty"`
	SunscreenRecommended bool     `json:"sunscreen_recommended"`
}

type psiBlock struct {
	Value      float64 `json:"value"`
	ObservedAt string  `json:"observed_at"`
}

type guidanceBlock struct {
	Version   string `json:"version"`
	Source    string `json:"source"`
	Retrieved string `json:"retrieved"`
}

type response struct {
	Status     string        `json:"status"`
	Reason     string        `json:"reason,omitempty"`
	Healthy    advice        `json:"healthy"`
	Vulnerable advice        `json:"vulnerable"`
	PM25       *pm25Block    `json:"pm25,omitempty"`
	Region     regionBlock   `json:"region"`
	UV         uvBlock       `json:"uv"`
	PSI24h     *psiBlock     `json:"psi_24h,omitempty"`
	Guidance   guidanceBlock `json:"guidance"`
	Disclaimer string        `json:"disclaimer"`
}

type errorBody struct {
	Error string `json:"error"`
}

const disclaimer = "Regional readings, not street-level. The advice is the official guidance as published; this is not medical advice."
