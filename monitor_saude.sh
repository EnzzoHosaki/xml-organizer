#!/bin/bash
# Script de Monitoramento Diário - XML Organizer v3.0
# Gera relatório completo de saúde do sistema

set -e

# Cores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuração
DB_FILE="/mnt/c/xml_organizer_data/xml_organizer.db"
QUARANTINE_DIR="/mnt/c/xml_organizer_data/quarantine"
DEAD_LETTER_DIR="/mnt/c/xml_organizer_data/dead_letter"
LOG_FILE="/mnt/c/xml_organizer_data/xml_organizer.log"

echo "======================================================================"
echo "  XML ORGANIZER - RELATÓRIO DE SAÚDE $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================================"
echo ""

# Função para executar query SQL
sql_query() {
    sqlite3 "$DB_FILE" "$1" 2>/dev/null || echo "N/A"
}

# 1. STATUS DO SERVIÇO
echo -e "${BLUE}1. STATUS DO SERVIÇO${NC}"
echo "----------------------------------------------------------------------"
if systemctl is-active --quiet xml-organizer 2>/dev/null; then
    echo -e "${GREEN}✓ Serviço ATIVO${NC}"
    uptime_seconds=$(systemctl show xml-organizer --property=ActiveEnterTimestamp --value | xargs -I {} date -d {} +%s 2>/dev/null || echo 0)
    current_seconds=$(date +%s)
    uptime_hours=$(( (current_seconds - uptime_seconds) / 3600 ))
    echo "  Uptime: $uptime_hours horas"
else
    echo -e "${RED}✗ Serviço INATIVO - ATENÇÃO REQUERIDA!${NC}"
fi
echo ""

# 2. PROCESSAMENTO HOJE
echo -e "${BLUE}2. PROCESSAMENTO HOJE${NC}"
echo "----------------------------------------------------------------------"

total_hoje=$(sql_query "SELECT COUNT(*) FROM processing_audit WHERE DATE(discovered_at) = DATE('now')")
sucesso_hoje=$(sql_query "SELECT COUNT(*) FROM processing_audit WHERE DATE(discovered_at) = DATE('now') AND current_status = 'SUCCESS'")
duplicado_hoje=$(sql_query "SELECT COUNT(*) FROM processing_audit WHERE DATE(discovered_at) = DATE('now') AND current_status = 'DUPLICATE'")
erro_hoje=$(sql_query "SELECT COUNT(*) FROM processing_audit WHERE DATE(discovered_at) = DATE('now') AND current_status NOT IN ('SUCCESS', 'DUPLICATE')")

echo "  Total processado: $total_hoje"
echo -e "  ${GREEN}Sucessos: $sucesso_hoje${NC}"
echo "  Duplicados: $duplicado_hoje"

if [ "$erro_hoje" -gt 0 ]; then
    echo -e "  ${YELLOW}Erros: $erro_hoje${NC}"
else
    echo -e "  ${GREEN}Erros: 0${NC}"
fi
echo ""

# 3. TAXA DE SUCESSO
echo -e "${BLUE}3. TAXA DE SUCESSO${NC}"
echo "----------------------------------------------------------------------"

if [ "$total_hoje" -gt 0 ]; then
    taxa_sucesso=$(sql_query "
        SELECT ROUND(100.0 * 
            SUM(CASE WHEN current_status = 'SUCCESS' THEN 1 ELSE 0 END) / 
            COUNT(*), 2)
        FROM processing_audit 
        WHERE DATE(discovered_at) = DATE('now')")
    
    echo "  Taxa de sucesso: ${taxa_sucesso}%"
    
    # Avaliar taxa
    taxa_int=$(echo "$taxa_sucesso" | cut -d. -f1)
    if [ "$taxa_int" -ge 99 ]; then
        echo -e "  ${GREEN}✓ EXCELENTE${NC} (≥99%)"
    elif [ "$taxa_int" -ge 95 ]; then
        echo -e "  ${YELLOW}⚠ ACEITÁVEL${NC} (95-99%)"
    else
        echo -e "  ${RED}✗ CRÍTICO${NC} (<95%) - INVESTIGAR IMEDIATAMENTE!"
    fi
else
    echo "  Nenhum arquivo processado hoje ainda"
fi
echo ""

# 4. TEMPO DE PROCESSAMENTO
echo -e "${BLUE}4. TEMPO DE PROCESSAMENTO${NC}"
echo "----------------------------------------------------------------------"

tempo_medio=$(sql_query "
    SELECT ROUND(AVG(total_duration_ms), 0)
    FROM processing_audit 
    WHERE current_status = 'SUCCESS' 
      AND DATE(discovered_at) = DATE('now')
      AND total_duration_ms IS NOT NULL")

tempo_max=$(sql_query "
    SELECT ROUND(MAX(total_duration_ms), 0)
    FROM processing_audit 
    WHERE current_status = 'SUCCESS' 
      AND DATE(discovered_at) = DATE('now')
      AND total_duration_ms IS NOT NULL")

if [ "$tempo_medio" != "N/A" ] && [ "$tempo_medio" != "" ]; then
    echo "  Tempo médio: ${tempo_medio}ms"
    echo "  Tempo máximo: ${tempo_max}ms"
    
    # Avaliar performance
    if [ "$tempo_medio" -lt 2000 ]; then
        echo -e "  ${GREEN}✓ EXCELENTE${NC} (<2s)"
    elif [ "$tempo_medio" -lt 5000 ]; then
        echo -e "  ${YELLOW}⚠ ACEITÁVEL${NC} (2-5s)"
    else
        echo -e "  ${YELLOW}⚠ LENTO${NC} (>5s) - Considerar otimização"
    fi
else
    echo "  Sem dados de performance ainda"
fi
echo ""

# 5. RETRY E FALHAS
echo -e "${BLUE}5. ANÁLISE DE RETRY${NC}"
echo "----------------------------------------------------------------------"

arquivos_retry=$(sql_query "
    SELECT COUNT(*) 
    FROM processing_audit 
    WHERE attempt_count > 1 
      AND DATE(discovered_at) = DATE('now')")

media_tentativas=$(sql_query "
    SELECT ROUND(AVG(attempt_count), 1)
    FROM processing_audit 
    WHERE DATE(discovered_at) = DATE('now')")

echo "  Arquivos que precisaram retry: $arquivos_retry"
echo "  Média de tentativas: $media_tentativas"

if [ "$media_tentativas" != "N/A" ] && [ "$media_tentativas" != "" ]; then
    media_int=$(echo "$media_tentativas" | cut -d. -f1)
    if [ "$media_int" -le 1 ]; then
        echo -e "  ${GREEN}✓ EXCELENTE${NC} (≤1.0)"
    elif [ "$media_int" -le 2 ]; then
        echo -e "  ${YELLOW}⚠ ACEITÁVEL${NC} (1.0-2.0)"
    else
        echo -e "  ${YELLOW}⚠ ALTO${NC} (>2.0) - Verificar estabilidade"
    fi
fi
echo ""

# 6. DEAD LETTER QUEUE
echo -e "${BLUE}6. DEAD LETTER QUEUE${NC}"
echo "----------------------------------------------------------------------"

dead_letter_count=$(ls -1 "$DEAD_LETTER_DIR"/*.xml 2>/dev/null | wc -l)

echo "  Arquivos em dead_letter: $dead_letter_count"

if [ "$dead_letter_count" -eq 0 ]; then
    echo -e "  ${GREEN}✓ EXCELENTE${NC} (nenhum arquivo com falha permanente)"
elif [ "$dead_letter_count" -le 3 ]; then
    echo -e "  ${YELLOW}⚠ ATENÇÃO${NC} (poucos arquivos - investigar)"
else
    echo -e "  ${RED}✗ CRÍTICO${NC} (muitos arquivos - AÇÃO REQUERIDA!)"
fi

# Listar arquivos se houver
if [ "$dead_letter_count" -gt 0 ]; then
    echo ""
    echo "  Arquivos em dead_letter:"
    ls -1 "$DEAD_LETTER_DIR"/*.xml 2>/dev/null | head -5 | while read file; do
        echo "    - $(basename "$file")"
    done
    if [ "$dead_letter_count" -gt 5 ]; then
        echo "    ... e mais $(($dead_letter_count - 5)) arquivo(s)"
    fi
fi
echo ""

# 7. QUARENTENA
echo -e "${BLUE}7. QUARENTENA${NC}"
echo "----------------------------------------------------------------------"

quarantine_count=$(ls -1 "$QUARANTINE_DIR"/*.xml 2>/dev/null | wc -l)
echo "  Arquivos em quarentena: $quarantine_count"

if [ "$quarantine_count" -eq 0 ]; then
    echo -e "  ${GREEN}✓ OK${NC} (vazio)"
elif [ "$quarantine_count" -le 5 ]; then
    echo -e "  ${GREEN}✓ OK${NC} (poucos arquivos temporários)"
else
    echo -e "  ${YELLOW}⚠ ATENÇÃO${NC} (muitos arquivos - verificar se sistema está processando)"
    
    # Verificar arquivos antigos
    old_files=$(find "$QUARANTINE_DIR" -name "*.xml" -mmin +10 2>/dev/null | wc -l)
    if [ "$old_files" -gt 0 ]; then
        echo -e "  ${RED}✗ $old_files arquivo(s) há mais de 10min - PROBLEMA!${NC}"
    fi
fi
echo ""

# 8. PRINCIPAIS ERROS
echo -e "${BLUE}8. PRINCIPAIS TIPOS DE ERRO (últimas 24h)${NC}"
echo "----------------------------------------------------------------------"

erros=$(sql_query "
    SELECT last_error_type, COUNT(*) as count
    FROM processing_audit
    WHERE current_status NOT IN ('SUCCESS', 'DUPLICATE')
      AND datetime(discovered_at) > datetime('now', '-24 hours')
    GROUP BY last_error_type
    ORDER BY count DESC
    LIMIT 5")

if [ "$erros" != "N/A" ] && [ "$erros" != "" ]; then
    echo "$erros" | while IFS='|' read -r error_type count; do
        echo "  ${error_type}: ${count}"
    done
else
    echo -e "  ${GREEN}✓ Nenhum erro nas últimas 24 horas${NC}"
fi
echo ""

# 9. RECONCILIAÇÃO
echo -e "${BLUE}9. ÚLTIMA RECONCILIAÇÃO${NC}"
echo "----------------------------------------------------------------------"

ultima_reconciliacao=$(sql_query "
    SELECT 
        run_at,
        files_checked,
        inconsistencies_found,
        issues_fixed
    FROM reconciliation_log
    ORDER BY id DESC LIMIT 1")

if [ "$ultima_reconciliacao" != "N/A" ] && [ "$ultima_reconciliacao" != "" ]; then
    echo "$ultima_reconciliacao" | IFS='|' read -r run_at files_checked inconsistencies issues_fixed
    echo "  Última execução: $run_at"
    echo "  Arquivos verificados: $files_checked"
    echo "  Inconsistências: $inconsistencies"
    echo "  Problemas corrigidos: $issues_fixed"
    
    if [ "$inconsistencies" -eq 0 ]; then
        echo -e "  ${GREEN}✓ Nenhuma inconsistência encontrada${NC}"
    else
        echo -e "  ${YELLOW}⚠ Inconsistências detectadas e corrigidas${NC}"
    fi
else
    echo "  Nenhuma reconciliação executada ainda"
fi
echo ""

# 10. ESTATÍSTICAS GERAIS
echo -e "${BLUE}10. ESTATÍSTICAS GERAIS (all time)${NC}"
echo "----------------------------------------------------------------------"

total_geral=$(sql_query "SELECT COUNT(*) FROM nota_fiscal")
empresas=$(sql_query "SELECT COUNT(*) FROM empresa")

echo "  Total de notas processadas: $total_geral"
echo "  Empresas cadastradas: $empresas"

# Distribuição por tipo
echo ""
echo "  Distribuição por tipo de documento:"
sql_query "
    SELECT tipo_documento, COUNT(*) as count
    FROM nota_fiscal
    GROUP BY tipo_documento
    ORDER BY count DESC" | while IFS='|' read -r tipo count; do
    echo "    $tipo: $count"
done
echo ""

# 11. SAÚDE DOS LOGS
echo -e "${BLUE}11. SAÚDE DOS LOGS${NC}"
echo "----------------------------------------------------------------------"

if [ -f "$LOG_FILE" ]; then
    log_size=$(du -h "$LOG_FILE" | cut -f1)
    log_lines=$(wc -l < "$LOG_FILE")
    echo "  Tamanho do log: $log_size"
    echo "  Linhas no log: $log_lines"
    
    # Últimos erros
    error_count=$(grep -c "ERROR\|CRITICAL" "$LOG_FILE" 2>/dev/null || echo 0)
    echo "  Linhas de erro: $error_count"
    
    if [ "$error_count" -gt 100 ]; then
        echo -e "  ${RED}✗ MUITOS ERROS${NC} - Investigar urgentemente"
    elif [ "$error_count" -gt 10 ]; then
        echo -e "  ${YELLOW}⚠ ALGUNS ERROS${NC} - Monitorar"
    else
        echo -e "  ${GREEN}✓ POUCOS ERROS${NC}"
    fi
else
    echo "  Log file não encontrado"
fi
echo ""

# 12. RECOMENDAÇÕES
echo -e "${BLUE}12. RECOMENDAÇÕES${NC}"
echo "----------------------------------------------------------------------"

recomendacoes=0

# Verificar taxa de sucesso
if [ "$total_hoje" -gt 0 ]; then
    taxa_int=$(echo "$taxa_sucesso" | cut -d. -f1)
    if [ "$taxa_int" -lt 95 ]; then
        echo -e "${RED}⚠ CRÍTICO: Taxa de sucesso < 95% - Investigar imediatamente${NC}"
        recomendacoes=$((recomendacoes + 1))
    fi
fi

# Verificar dead letter
if [ "$dead_letter_count" -gt 5 ]; then
    echo -e "${RED}⚠ CRÍTICO: Dead Letter Queue com muitos arquivos - Ação requerida${NC}"
    recomendacoes=$((recomendacoes + 1))
fi

# Verificar quarentena
old_quarantine=$(find "$QUARANTINE_DIR" -name "*.xml" -mmin +10 2>/dev/null | wc -l)
if [ "$old_quarantine" -gt 0 ]; then
    echo -e "${RED}⚠ CRÍTICO: Arquivos presos em quarentena - Verificar serviço${NC}"
    recomendacoes=$((recomendacoes + 1))
fi

# Verificar serviço
if ! systemctl is-active --quiet xml-organizer 2>/dev/null; then
    echo -e "${RED}⚠ CRÍTICO: Serviço parado - Iniciar imediatamente${NC}"
    recomendacoes=$((recomendacoes + 1))
fi

if [ "$recomendacoes" -eq 0 ]; then
    echo -e "${GREEN}✓ Sistema funcionando corretamente - Nenhuma ação necessária${NC}"
fi
echo ""

# RESUMO FINAL
echo "======================================================================"
echo -e "${BLUE}RESUMO FINAL${NC}"
echo "======================================================================"

if [ "$recomendacoes" -eq 0 ]; then
    if [ "$total_hoje" -gt 0 ]; then
        taxa_int=$(echo "$taxa_sucesso" | cut -d. -f1)
        if [ "$taxa_int" -ge 99 ] && [ "$dead_letter_count" -eq 0 ]; then
            echo -e "${GREEN}🟢 SISTEMA SAUDÁVEL${NC} - Operando perfeitamente"
        else
            echo -e "${YELLOW}🟡 SISTEMA OPERACIONAL${NC} - Funcionando normalmente"
        fi
    else
        echo -e "${BLUE}🔵 AGUARDANDO DADOS${NC} - Nenhum arquivo processado hoje ainda"
    fi
else
    echo -e "${RED}🔴 ATENÇÃO REQUERIDA${NC} - $recomendacoes problema(s) detectado(s)"
fi

echo ""
echo "Relatório gerado em: $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================================================"
