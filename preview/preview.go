package preview

import (
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/utils"
)

func LoginPrefix(login string) string {
	return login + ": "
}

func CreateMessagePreview(cleanTagsPolicy *sanitizer.StripTagsPolicy, previewMaxTextSize int, text, login string) string {
	input := LoginPrefix(login) + text
	return CreateMessagePreviewWithoutLogin(cleanTagsPolicy, previewMaxTextSize, input)
}

func CreateMessagePreviewWithoutLogin(cleanTagsPolicy *sanitizer.StripTagsPolicy, previewMaxTextSize int, text string) string {
	return stripTagsAndCut(cleanTagsPolicy, previewMaxTextSize, text)
}

func stripTagsAndCut(cleanTagsPolicy *sanitizer.StripTagsPolicy, sizeToCut int, text string) string {
	tmp := cleanTagsPolicy.Sanitize(text)
	runes := []rune(tmp)
	textLen := len(runes)
	size := utils.Min(sizeToCut, textLen)
	ret := string(runes[:size])
	if textLen > sizeToCut {
		ret += "..."
	}
	return ret
}
