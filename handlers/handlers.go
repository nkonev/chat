package handlers

import (
	"context"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/fx"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func bindHttpHandlers(
	ginRouter *gin.Engine,
	chatHandler *ChatHandler,
	participantHandler *ParticipantHandler,
	messageHandler *MessageHandler,
	blogHandler *BlogHandler,
) {
	ginRouter.POST("/chat", chatHandler.CreateChat)
	ginRouter.PUT("/chat", chatHandler.EditChat)
	ginRouter.DELETE("/chat/:id", chatHandler.DeleteChat)
	ginRouter.PUT("/chat/:id/pin", chatHandler.PinChat)
	ginRouter.GET("/chat/search", chatHandler.SearchChats)

	ginRouter.PUT("/chat/:id/participant", participantHandler.AddParticipant)
	ginRouter.DELETE("/chat/:id/participant", participantHandler.DeleteParticipant)
	ginRouter.GET("/chat/:id/participants", participantHandler.GetParticipants)
	ginRouter.PUT("/chat/:id/participant/:participantId", participantHandler.ChangeParticipant)

	ginRouter.POST("/chat/:id/message", messageHandler.CreateMessage)
	ginRouter.PUT("/chat/:id/message", messageHandler.EditMessage)
	ginRouter.DELETE("/chat/:id/message/:messageId", messageHandler.DeleteMessage)
	ginRouter.PUT("/chat/:id/message/:messageId/read", messageHandler.ReadMessage)
	ginRouter.GET("/chat/:id/message/search", messageHandler.SearchMessages)
	ginRouter.PUT("/chat/:id/message/:messageId/blog-post", messageHandler.MakeBlogPost)

	ginRouter.GET("/blog/search", blogHandler.SearchBlogs)
	ginRouter.GET("/blog/:id", blogHandler.GetBlog)
	ginRouter.GET("/blog/:id/comment/search", blogHandler.SearchComments)

	ginRouter.GET("/internal/health", func(g *gin.Context) {
		g.Status(http.StatusOK)
	})
}

func getUserId(g *gin.Context) (int64, error) {
	uh := g.Request.Header.Get("X-UserId")
	return utils.ParseInt64(uh)
}

func ConfigureHttpServer(
	cfg *config.AppConfig,
	lgr *logger.LoggerWrapper,
	lc fx.Lifecycle,
	chatHandler *ChatHandler,
	participantHandler *ParticipantHandler,
	messageHandler *MessageHandler,
	blogHandler *BlogHandler,
) *http.Server {
	// https://gin-gonic.com/en/docs/examples/graceful-restart-or-stop/
	gin.SetMode(gin.ReleaseMode)
	ginRouter := gin.New()
	ginRouter.Use(otelgin.Middleware(app.TRACE_RESOURCE))
	ginRouter.Use(StructuredLogMiddleware(lgr))
	ginRouter.Use(WriteTraceToHeaderMiddleware())
	ginRouter.Use(gin.Recovery())

	bindHttpHandlers(ginRouter, chatHandler, participantHandler, messageHandler, blogHandler)

	httpServer := &http.Server{
		Addr:           cfg.HttpServerConfig.Address,
		Handler:        ginRouter.Handler(),
		ReadTimeout:    cfg.HttpServerConfig.ReadTimeout,
		WriteTimeout:   cfg.HttpServerConfig.WriteTimeout,
		MaxHeaderBytes: cfg.HttpServerConfig.MaxHeaderBytes,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Stopping http server")

			if err := httpServer.Shutdown(context.Background()); err != nil {
				lgr.Error("Error shutting http server", "err", err)
			}
			return nil
		},
	})

	return httpServer
}
func StructuredLogMiddleware(lgr *logger.LoggerWrapper) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		traceId := logger.GetTraceId(ctx)

		// Start timer
		start := time.Now()

		// Process Request
		c.Next()

		// Stop timer
		end := time.Now()

		duration := end.Sub(start)

		entries := []any{
			"client_ip", c.ClientIP(),
			"duration", duration,
			"method", c.Request.Method,
			"path", c.Request.RequestURI,
			"status", c.Writer.Status(),
			"referrer", c.Request.Referer(),
			logger.LogFieldTraceId, traceId,
		}

		if c.Writer.Status() >= 500 {
			lgr.Error("Request", entries...)
		} else {
			lgr.Info("Request", entries...)
		}
	}
}

func WriteTraceToHeaderMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := logger.GetTraceId(c.Request.Context())

		c.Writer.Header().Set("trace-id", traceId)

		// Process Request
		c.Next()

	}
}

func RunHttpServer(
	lgr *logger.LoggerWrapper,
	httpServer *http.Server,
) {
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			lgr.Info("Http server is closed")
		} else if err != nil {
			lgr.Error("Got http server error", "err", err)
		}
	}()
}

func TrimAmdSanitizeChatTitle(policy *services.StripTagsPolicy, title string) string {
	t := Trim(policy.Sanitize(title))
	return t
}

func Trim(str string) string {
	return strings.TrimSpace(str)
}

func SanitizeMessage(policy *services.SanitizerPolicy, input string) string {
	return policy.Sanitize(input)
}

func TrimAmdSanitizeMessage(ctx context.Context, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *services.SanitizerPolicy, input string) (string, error) {
	sanitizedHtml := Trim(SanitizeMessage(policy, input))

	whitelist := cfg.MessageConfig.AllowedMediaUrls
	wlArr := strings.Split(whitelist, ",")
	frontendUrl := cfg.FrontendUrl
	wlArr = append(wlArr, frontendUrl)
	wlArr = append(wlArr, "") // storage urls without protocol://host:port

	iframeWhitelist := cfg.MessageConfig.AllowedIframeUrls
	iframeWlArr := strings.Split(iframeWhitelist, ",")

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(sanitizedHtml))
	if err != nil {
		lgr.WarnContext(ctx, "Unable to read html", "err", err)
		return "", errors.New("Unable to read html")
	}

	var retErr error
	maxMediasCount := cfg.MessageConfig.MaxMedias
	mediaCount := 0

	doc.Find("img").Each(func(i int, s *goquery.Selection) {
		maybeImage := s.First()
		if maybeImage != nil {
			src, exists := maybeImage.Attr("src")
			if exists && !utils.ContainsUrl(ctx, lgr, wlArr, src) {
				lgr.InfoContext(ctx, "Filtered not allowed url in image src", "src", src)
				retErr = &MediaUrlErr{src, "image src"}
			}
			if exists {
				fixedSrc, err := removeProtocolHostPortIfNeed(src, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeImage.SetAttr("src", fixedSrc)
			}

			original, originalExists := maybeImage.Attr("data-original")
			if originalExists && (!utils.ContainsUrl(ctx, lgr, wlArr, original) && !utils.ContainsUrl(ctx, lgr, iframeWlArr, original)) {
				lgr.InfoContext(ctx, "Filtered not allowed url in image src", "src", original)
				retErr = &MediaUrlErr{original, "image src"}
			}
			if originalExists {
				fixedSrc, err := removeProtocolHostPortIfNeed(original, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeImage.SetAttr("data-original", fixedSrc)
			}

			mediaCount++
		}
	})
	if retErr != nil {
		return "", retErr
	}

	if mediaCount > maxMediasCount {
		retErr = &MediaOverflowErr{maxMediasCount, mediaCount}
		return "", retErr
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		maybeA := s.First()
		if maybeA != nil {
			src, exists := maybeA.Attr("href")
			if exists {
				fixedSrc, err := removeProtocolHostPortIfNeed(src, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeA.SetAttr("href", fixedSrc)
			}
		}
	})
	if retErr != nil {
		return "", retErr
	}

	ret, err := doc.Find("html").Find("body").Html()
	if err != nil {
		lgr.WarnContext(ctx, "Unable to write html", "err", err)
		return "", err
	}

	return ret, nil
}

type MediaUrlErr struct {
	url   string
	where string
}

func (s *MediaUrlErr) Error() string {
	return fmt.Sprintf("Media url is not allowed in %v: %v", s.where, s.url)
}

type MediaOverflowErr struct {
	allowed int
	given   int
}

func (s *MediaOverflowErr) Error() string {
	return fmt.Sprintf("Too many medias: allowed %v, given %v", s.allowed, s.given)
}

func removeProtocolHostPortIfNeed(src, frontendUrl string) (string, error) {
	parsed, err := url.Parse(src)
	if err != nil {
		return "", err
	}

	parsedAllowedUrl, err := url.Parse(frontendUrl)
	if err != nil {
		return "", err
	}

	if utils.ContainUrl(parsed, parsedAllowedUrl) {
		parsed.Host = ""
		parsed.Scheme = ""
		parsed.User = nil
	}
	return parsed.String(), nil
}
