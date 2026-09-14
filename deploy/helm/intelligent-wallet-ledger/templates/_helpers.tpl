{{- define "intelligent-wallet-ledger.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "intelligent-wallet-ledger.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "intelligent-wallet-ledger.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "intelligent-wallet-ledger.labels" -}}
app.kubernetes.io/name: {{ include "intelligent-wallet-ledger.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "intelligent-wallet-ledger.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "intelligent-wallet-ledger.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create is false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "intelligent-wallet-ledger.image" -}}
{{- $repository := required "image.repository is required" .Values.image.repository }}
{{- $digest := required "image.digest is required and must be an immutable SHA-256 digest" .Values.image.digest }}
{{- printf "%s@%s" $repository $digest }}
{{- end }}
