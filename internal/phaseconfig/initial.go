package phaseconfig

func InsertInitialPrompt(pre *[]Phase, prompt string) {
	*pre = append([]Phase{{Name: "(initial)", Prompt: prompt}}, *pre...)
}