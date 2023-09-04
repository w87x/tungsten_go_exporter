package main

type named_href struct {
	Name string `json:"name"`
	Href string `json:"href"`
}
type Config struct {
	LogLevel string          `yaml:"loglevel"`
	Web      web_config      `yaml:"web"`
	Keystone keystone_config `yaml:"keystone"`
	Scrape   scrape_config   `yaml:"scrape"`
}
type web_config struct {
	Listen string `yaml:"listen"`
}
type keystone_config struct {
	Url           string `yaml:"url"`
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	Domain        string `yaml:"domain"`
	Project       string `yaml:"project"`
	ProjectDomain string `yaml:"project_domain"`
	Tls           bool   `yaml:"tls"`
}
type scrape_config struct {
	Url      string `yaml:"url"`
	Retries  int    `yaml:"retries"`
	Timeout  int    `yaml:"timeout"`
	Threads  int    `yaml:"threads"`
	Interval int    `yaml:"interval"`
}
