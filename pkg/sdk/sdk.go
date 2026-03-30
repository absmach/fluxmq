// Copyright (c) Abstract Machines
// SPDX-License-Identifier: Apache-2.0

package sdk

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	smqerrors "github.com/absmach/supermq/pkg/errors"
	smqSDK "github.com/absmach/supermq/pkg/sdk"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"moul.io/http2curl"
)

var _ SDK = (*mgSDK)(nil)

type Metadata map[string]any

type PageMetadata struct {
	Total          uint64    `json:"total"`
	Offset         uint64    `json:"offset"`
	Limit          uint64    `json:"limit"`
	Metadata       Metadata  `json:"metadata,omitempty"`
	Topic          string    `json:"topic,omitempty"`
	Contact        string    `json:"contact,omitempty"`
	DomainID       string    `json:"domain_id,omitempty"`
	Level          uint64    `json:"level,omitempty"`
	State          string    `json:"state,omitempty"`
	Name           string    `json:"name,omitempty"`
	Status         string    `json:"status,omitempty"`
	Dir            string    `json:"dir,omitempty"`
	Order          string    `json:"order,omitempty"`
	Tag            string    `json:"tag,omitempty"`
	InputChannel   string    `json:"input_channel,omitempty"`
	RuleID         string    `json:"rule_id,omitempty"`
	ChannelID      string    `json:"channel_id,omitempty"`
	ClientID       string    `json:"client_id,omitempty"`
	Subtopic       string    `json:"subtopic,omitempty"`
	AssigneeID     string    `json:"assignee_id,omitempty"`
	Severity       uint8     `json:"severity,omitempty"`
	UpdatedBy      string    `json:"updated_by,omitempty"`
	AssignedBy     string    `json:"assigned_by,omitempty"`
	AcknowledgedBy string    `json:"acknowledged_by,omitempty"`
	ResolvedBy     string    `json:"resolved_by,omitempty"`
	CreatedFrom    time.Time `json:"created_from,omitempty"`
	CreatedTo      time.Time `json:"created_to,omitempty"`
}

type MessagePageMetadata struct {
	PageMetadata
	Subtopic    string  `json:"subtopic,omitempty"`
	Publisher   string  `json:"publisher,omitempty"`
	Limit       int     `json:"limit,omitempty"`
	Name        string  `json:"name,omitempty"`
	Comparator  string  `json:"comparator,omitempty"`
	BoolValue   *bool   `json:"vb,omitempty"`
	StringValue string  `json:"vs,omitempty"`
	DataValue   string  `json:"vd,omitempty"`
	From        float64 `json:"from,omitempty"`
	To          float64 `json:"to,omitempty"`
	Aggregation string  `json:"aggregation,omitempty"`
	Interval    string  `json:"interval,omitempty"`
	Value       float64 `json:"value,omitempty"`
	Protocol    string  `json:"protocol,omitempty"`
}

// SDK contains Magistrala API.
type SDK interface {
	smqSDK.SDK

	// AddBootstrap add bootstrap configuration
	//
	// example:
	//  cfg := sdk.BootstrapConfig{
	//    ClientID: "clientID",
	//    Name: "bootstrap",
	//    ExternalID: "externalID",
	//    ExternalKey: "externalKey",
	//    Channels: []string{"channel1", "channel2"},
	//  }
	//  id, _ := sdk.AddBootstrap(ctx, cfg, "domainID", "token")
	//  fmt.Println(id)
	AddBootstrap(ctx context.Context, cfg BootstrapConfig, domainID, token string) (string, smqerrors.SDKError)

	// View returns Client Config with given ID belonging to the user identified by the given token.
	//
	// example:
	//  bootstrap, _ := sdk.ViewBootstrap(ctx, "id", "domainID", "token")
	//  fmt.Println(bootstrap)
	ViewBootstrap(ctx context.Context, id, domainID, token string) (BootstrapConfig, smqerrors.SDKError)

	// Update updates editable fields of the provided Config.
	//
	// example:
	//  cfg := sdk.BootstrapConfig{
	//    ClientID: "clientID",
	//    Name: "bootstrap",
	//    ExternalID: "externalID",
	//    ExternalKey: "externalKey",
	//    Channels: []string{"channel1", "channel2"},
	//  }
	//  err := sdk.UpdateBootstrap(ctx, cfg, "domainID", "token")
	//  fmt.Println(err)
	UpdateBootstrap(ctx context.Context, cfg BootstrapConfig, domainID, token string) smqerrors.SDKError

	// Update bootstrap config certificates.
	//
	// example:
	//  err := sdk.UpdateBootstrapCerts(ctx, "id", "clientCert", "clientKey", "ca", "domainID", "token")
	//  fmt.Println(err)
	UpdateBootstrapCerts(ctx context.Context, id string, clientCert, clientKey, ca string, domainID, token string) (BootstrapConfig, smqerrors.SDKError)

	// UpdateBootstrapConnection updates connections performs update of the channel list corresponding Client is connected to.
	//
	// example:
	//  err := sdk.UpdateBootstrapConnection(ctx, "id", []string{"channel1", "channel2"}, "domainID", "token")
	//  fmt.Println(err)
	UpdateBootstrapConnection(ctx context.Context, id string, channels []string, domainID, token string) smqerrors.SDKError

	// Remove removes Config with specified token that belongs to the user identified by the given token.
	//
	// example:
	//  err := sdk.RemoveBootstrap(ctx, "id", "domainID", "token")
	//  fmt.Println(err)
	RemoveBootstrap(ctx context.Context, id, domainID, token string) smqerrors.SDKError

	// Bootstrap returns Config to the Client with provided external ID using external key.
	//
	// example:
	//  bootstrap, _ := sdk.Bootstrap(ctx, "externalID", "externalKey")
	//  fmt.Println(bootstrap)
	Bootstrap(ctx context.Context, externalID, externalKey string) (BootstrapConfig, smqerrors.SDKError)

	// BootstrapSecure retrieves a configuration with given external ID and encrypted external key.
	//
	// example:
	//  bootstrap, _ := sdk.BootstrapSecure(ctx, "externalID", "externalKey", "cryptoKey")
	//  fmt.Println(bootstrap)
	BootstrapSecure(ctx context.Context, externalID, externalKey, cryptoKey string) (BootstrapConfig, smqerrors.SDKError)

	// Bootstraps retrieves a list of managed configs.
	//
	// example:
	//  pm := sdk.PageMetadata{
	//    Offset: 0,
	//    Limit:  10,
	//  }
	//  bootstraps, _ := sdk.Bootstraps(ctx, pm, "domainID", "token")
	//  fmt.Println(bootstraps)
	Bootstraps(ctx context.Context, pm PageMetadata, domainID, token string) (BootstrapPage, smqerrors.SDKError)

	// Whitelist updates Client state Config with given ID belonging to the user identified by the given token.
	//
	// example:
	//  err := sdk.Whitelist(ctx, "clientID", 1, "domainID", "token")
	//  fmt.Println(err)
	Whitelist(ctx context.Context, clientID string, state int, domainID, token string) smqerrors.SDKError

	// ReadMessages read messages of specified channel.
	//
	// example:
	//  pm := sdk.MessagePageMetadata{
	//    Offset: 0,
	//    Limit:  10,
	//  }
	//  msgs, _ := sdk.ReadMessages(ctx, pm,"channelID", "domainID", "token")
	//  fmt.Println(msgs)
	ReadMessages(ctx context.Context, pm MessagePageMetadata, chanID, domainID, token string) (MessagesPage, smqerrors.SDKError)

	// CreateSubscription creates a new subscription
	//
	// example:
	//  subscription, _ := sdk.CreateSubscription(ctx, "topic", "contact", "token")
	//  fmt.Println(subscription)
	CreateSubscription(ctx context.Context, topic, contact, token string) (string, smqerrors.SDKError)

	// ListSubscriptions list subscriptions given list parameters.
	//
	// example:
	//  pm := sdk.PageMetadata{
	//    Offset: 0,
	//    Limit:  10,
	//  }
	//  subscriptions, _ := sdk.ListSubscriptions(ctx, pm, "token")
	//  fmt.Println(subscriptions)
	ListSubscriptions(ctx context.Context, pm PageMetadata, token string) (SubscriptionPage, smqerrors.SDKError)

	// ViewSubscription retrieves a subscription with the provided id.
	//
	// example:
	//  subscription, _ := sdk.ViewSubscription(ctx, "id", "token")
	//  fmt.Println(subscription)
	ViewSubscription(ctx context.Context, id, token string) (Subscription, smqerrors.SDKError)

	// DeleteSubscription removes a subscription with the provided id.
	//
	// example:
	//  err := sdk.DeleteSubscription(ctx, "id", "token")
	//  fmt.Println(err)
	DeleteSubscription(ctx context.Context, id, token string) smqerrors.SDKError

	// Alarms API

	// UpdateAlarm updates an existing alarm.
	UpdateAlarm(ctx context.Context, alarm Alarm, domainID, token string) (Alarm, smqerrors.SDKError)

	// ViewAlarm retrieves an alarm by its ID.
	ViewAlarm(ctx context.Context, id, domainID, token string) (Alarm, smqerrors.SDKError)

	// ListAlarms retrieves a page of alarms.
	ListAlarms(ctx context.Context, pm PageMetadata, domainID, token string) (AlarmsPage, smqerrors.SDKError)

	// DeleteAlarm deletes an alarm.
	DeleteAlarm(ctx context.Context, id, domainID, token string) smqerrors.SDKError

	// Reports API

	// AddReportConfig creates a new report configuration.
	AddReportConfig(ctx context.Context, cfg ReportConfig, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// ViewReportConfig retrieves a report config by its ID.
	ViewReportConfig(ctx context.Context, id, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// UpdateReportConfig updates an existing report configuration.
	UpdateReportConfig(ctx context.Context, cfg ReportConfig, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// UpdateReportSchedule updates an existing report configuration's schedule.
	UpdateReportSchedule(ctx context.Context, cfg ReportConfig, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// RemoveReportConfig deletes a report config.
	RemoveReportConfig(ctx context.Context, id, domainID, token string) smqerrors.SDKError

	// ListReportsConfig retrieves a page of report configs.
	ListReportsConfig(ctx context.Context, pm PageMetadata, domainID, token string) (ReportConfigPage, smqerrors.SDKError)

	// EnableReportConfig enables a report config.
	EnableReportConfig(ctx context.Context, id, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// DisableReportConfig disables a report config.
	DisableReportConfig(ctx context.Context, id, domainID, token string) (ReportConfig, smqerrors.SDKError)

	// UpdateReportTemplate updates a report template.
	UpdateReportTemplate(ctx context.Context, cfg ReportConfig, domainID, token string) smqerrors.SDKError

	// ViewReportTemplate retrieves a report template.
	ViewReportTemplate(ctx context.Context, id, domainID, token string) (ReportTemplate, smqerrors.SDKError)

	// DeleteReportTemplate deletes a report template.
	DeleteReportTemplate(ctx context.Context, id, domainID, token string) smqerrors.SDKError

	// GenerateReport generates a report from a configuration.
	GenerateReport(ctx context.Context, config ReportConfig, action ReportAction, domainID, token string) (ReportPage, *ReportFile, smqerrors.SDKError)
	// Rules Engine API

	// AddRule creates a new rule.
	AddRule(ctx context.Context, r Rule, domainID, token string) (Rule, smqerrors.SDKError)

	// ViewRule retrieves a rule by its ID.
	ViewRule(ctx context.Context, id, domainID, token string) (Rule, smqerrors.SDKError)

	// UpdateRule updates an existing rule.
	UpdateRule(ctx context.Context, r Rule, domainID, token string) (Rule, smqerrors.SDKError)

	// UpdateRuleTags updates an existing rule's tags.
	UpdateRuleTags(ctx context.Context, r Rule, domainID, token string) (Rule, smqerrors.SDKError)

	// UpdateRuleSchedule updates an existing rule's schedule.
	UpdateRuleSchedule(ctx context.Context, r Rule, domainID, token string) (Rule, smqerrors.SDKError)

	// ListRules retrieves a page of rules.
	ListRules(ctx context.Context, pm PageMetadata, domainID, token string) (Page, smqerrors.SDKError)

	// RemoveRule deletes a rule.
	RemoveRule(ctx context.Context, id, domainID, token string) smqerrors.SDKError

	// EnableRule enables a rule.
	EnableRule(ctx context.Context, id, domainID, token string) (Rule, smqerrors.SDKError)

	// DisableRule disables a rule.
	DisableRule(ctx context.Context, id, domainID, token string) (Rule, smqerrors.SDKError)
}

type mgSDK struct {
	bootstrapURL   string
	readersURL     string
	usersURL       string
	alarmsURL      string
	reportsURL     string
	rulesEngineURL string
	client         *http.Client
	curlFlag       bool
	msgContentType smqSDK.ContentType

	smqSDK.SDK
}

// Config contains sdk configuration parameters.
type Config struct {
	BootstrapURL   string
	CertsURL       string
	HTTPAdapterURL string
	ReaderURL      string
	ClientsURL     string
	UsersURL       string
	GroupsURL      string
	ChannelsURL    string
	DomainsURL     string
	JournalURL     string
	HostURL        string
	AlarmsURL      string
	ReportsURL     string
	RulesEngineURL string

	MsgContentType  smqSDK.ContentType
	TLSVerification bool
	CurlFlag        bool
}

// NewSDK returns new supermq SDK instance.
func NewSDK(conf Config) SDK {
	smqSDK := smqSDK.NewSDK(smqSDK.Config{
		CertsURL:       conf.CertsURL,
		HTTPAdapterURL: conf.HTTPAdapterURL,
		ClientsURL:     conf.ClientsURL,
		UsersURL:       conf.UsersURL,
		GroupsURL:      conf.GroupsURL,
		ChannelsURL:    conf.ChannelsURL,
		DomainsURL:     conf.DomainsURL,
		JournalURL:     conf.JournalURL,
		HostURL:        conf.HostURL,

		MsgContentType:  conf.MsgContentType,
		TLSVerification: conf.TLSVerification,
		CurlFlag:        conf.CurlFlag,
	})

	return &mgSDK{
		bootstrapURL:   conf.BootstrapURL,
		readersURL:     conf.ReaderURL,
		usersURL:       conf.UsersURL,
		alarmsURL:      conf.AlarmsURL,
		reportsURL:     conf.ReportsURL,
		rulesEngineURL: conf.RulesEngineURL,
		msgContentType: conf.MsgContentType,

		client: &http.Client{Transport: otelhttp.NewTransport(&http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: !conf.TLSVerification,
			},
			IdleConnTimeout: 90 * time.Second,
		})},
		curlFlag: conf.CurlFlag,
		SDK:      smqSDK,
	}
}

// processRequest creates and send a new HTTP request, and checks for errors in the HTTP response.
// It then returns the response headers, the response body, and the associated error(s) (if any).
func (sdk mgSDK) processRequest(ctx context.Context, method, reqUrl, token string, data []byte, headers map[string]string, expectedRespCodes ...int) (http.Header, []byte, smqerrors.SDKError) {
	req, err := http.NewRequestWithContext(ctx, method, reqUrl, bytes.NewReader(data))
	if err != nil {
		return make(http.Header), []byte{}, smqerrors.NewSDKError(err)
	}

	// Sets a default value for the Content-Type.
	// Overridden if Content-Type is passed in the headers arguments.
	req.Header.Add("Content-Type", string(smqSDK.CTJSON))

	for key, value := range headers {
		req.Header.Add(key, value)
	}

	if token != "" {
		if !strings.Contains(token, smqSDK.ClientPrefix) {
			token = smqSDK.BearerPrefix + token
		}
		req.Header.Set("Authorization", token)
	}

	if sdk.curlFlag {
		curlCommand, err := http2curl.GetCurlCommand(req)
		if err != nil {
			return nil, nil, smqerrors.NewSDKError(err)
		}
		log.Println(curlCommand.String())
	}

	resp, err := sdk.client.Do(req)
	if err != nil {
		var opErr *net.OpError
		switch {
		case errors.Is(err, syscall.ECONNRESET):
			return make(http.Header), []byte{}, smqerrors.NewSDKError(fmt.Errorf("request failed: connection reset by peer: %w", err))
		case errors.As(err, &opErr):
			return make(http.Header), []byte{}, smqerrors.NewSDKError(fmt.Errorf("request failed: network error (%s): %w", opErr.Op, err))
		case errors.Is(err, io.EOF):
			return make(http.Header), []byte{}, smqerrors.NewSDKError(fmt.Errorf("request failed: connection closed unexpectedly: %w", err))
		default:
			return make(http.Header), []byte{}, smqerrors.NewSDKError(fmt.Errorf("request failed: %w", err))
		}
	}
	defer resp.Body.Close()

	sdkerr := smqerrors.CheckError(resp, expectedRespCodes...)
	if sdkerr != nil {
		return make(http.Header), []byte{}, sdkerr
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return make(http.Header), []byte{}, smqerrors.NewSDKError(err)
	}

	return resp.Header, body, nil
}

func (sdk mgSDK) withQueryParams(baseURL, endpoint string, pm PageMetadata) (string, error) {
	q, err := pm.query()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s/%s?%s", baseURL, endpoint, q), nil
}

func (pm PageMetadata) query() (string, error) {
	q := url.Values{}
	if pm.Offset != 0 {
		q.Add("offset", strconv.FormatUint(pm.Offset, 10))
	}
	if pm.Limit != 0 {
		q.Add("limit", strconv.FormatUint(pm.Limit, 10))
	}
	if pm.Total != 0 {
		q.Add("total", strconv.FormatUint(pm.Total, 10))
	}
	if pm.Metadata != nil {
		md, err := json.Marshal(pm.Metadata)
		if err != nil {
			return "", smqerrors.NewSDKError(err)
		}
		q.Add("metadata", string(md))
	}
	if pm.Topic != "" {
		q.Add("topic", pm.Topic)
	}
	if pm.Contact != "" {
		q.Add("contact", pm.Contact)
	}
	if pm.DomainID != "" {
		q.Add("domain_id", pm.DomainID)
	}
	if pm.Level != 0 {
		q.Add("level", strconv.FormatUint(pm.Level, 10))
	}
	if pm.Name != "" {
		q.Add("name", pm.Name)
	}
	if pm.Status != "" {
		q.Add("status", pm.Status)
	}
	if pm.Dir != "" {
		q.Add("dir", pm.Dir)
	}
	if pm.Order != "" {
		q.Add("order", pm.Order)
	}
	if pm.Tag != "" {
		q.Add("tag", pm.Tag)
	}
	if pm.InputChannel != "" {
		q.Add("input_channel", pm.InputChannel)
	}
	if pm.RuleID != "" {
		q.Add("rule_id", pm.RuleID)
	}
	if pm.ChannelID != "" {
		q.Add("channel_id", pm.ChannelID)
	}
	if pm.ClientID != "" {
		q.Add("client_id", pm.ClientID)
	}
	if pm.Subtopic != "" {
		q.Add("subtopic", pm.Subtopic)
	}
	if pm.AssigneeID != "" {
		q.Add("assignee_id", pm.AssigneeID)
	}
	if pm.Severity != 0 {
		q.Add("severity", strconv.FormatUint(uint64(pm.Severity), 10))
	}
	if pm.UpdatedBy != "" {
		q.Add("updated_by", pm.UpdatedBy)
	}
	if pm.AssignedBy != "" {
		q.Add("assigned_by", pm.AssignedBy)
	}
	if pm.AcknowledgedBy != "" {
		q.Add("acknowledged_by", pm.AcknowledgedBy)
	}
	if pm.ResolvedBy != "" {
		q.Add("resolved_by", pm.ResolvedBy)
	}
	if !pm.CreatedFrom.IsZero() {
		q.Add("created_from", pm.CreatedFrom.UTC().Format(time.RFC3339))
	}
	if !pm.CreatedTo.IsZero() {
		q.Add("created_to", pm.CreatedTo.UTC().Format(time.RFC3339))
	}

	return q.Encode(), nil
}
