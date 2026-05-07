{{/*
Expand the name of the chart.
*/}}
{{- define "aurekai-runtime.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "aurekai-runtime.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s" (include "aurekai-runtime.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
