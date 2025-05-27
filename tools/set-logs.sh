#!/bin/bash

# Vérifier si la variable d'environment RUST_LOG est définie, sinon utiliser une valeur par défaut
if [[ -z "$RUST_LOG" ]]; then
  RUST_LOG="info"
  echo "RUST_LOG is not set. Using default: $RUST_LOG"
fi

# Liste des modules définis dans le script
hyle_modules=(
  "hyle::indexer"
  "hyle::mempool"
  "hyle::mempool::storage"
  "hyle::p2p"
  "hyle::node_state"
  "hyle::data_availability"
  "hyle::single_node_consensus"
  "hyle::consensus"
  "sqlx::query"
  "risc0_zkvm"
  "tower_http"
)

# Fonction pour extraire le niveau de log d'un module depuis RUST_LOG
get_level_for_module() {
  module=$1
  # Recherche du niveau de log pour le module dans RUST_LOG
  # echo "$RUST_LOG" | grep -oP "(?<=^|\s)$module=\K[^,]*"
  echo "$RUST_LOG" | sed -n "s/.*\b$module=\([^,]*\).*/\1/p"
}

get_global_level() {
  echo "$RUST_LOG" | sed 's/\([^,]*\).*/\1/'
}

# Fonction pour afficher la configuration actuelle de RUST_LOG
show_config() {
  echo "Current RUST_LOG config: $RUST_LOG"
}

# Ajouter ou modifier un module dans RUST_LOG
add_module() {
  # Préparer la liste des modules avec leur niveau actuel
  module_list=()
  
  # Pour chaque module défini dans le script, obtenir son niveau actuel
  for module in "${hyle_modules[@]}"; do
    current_level=$(get_level_for_module "$module")
    
    # Si le module n'a pas de niveau de log défini, utiliser un niveau par défaut
    if [[ -z "$current_level" ]]; then
      current_level=$(get_global_level)
    fi
    
    module_list+=("$module ($current_level)")
  done

  # Utilisation de fzf pour choisir un module à modifier ou ajouter
  selected_module=$(printf "%s\n" "${module_list[@]}" | fzf --prompt="Select a module to modify/add: ")

  # Si un module est sélectionné
  if [[ -n "$selected_module" ]]; then
    # Extraire le nom du module et le niveau actuel
    module_name=$(echo "$selected_module" | sed 's/ (\([^)]*\))$//')
    current_level=$(echo "$selected_module" | sed 's/^[^(]* (\([^)]*\))$/\1/')

    # Demander de choisir ou de modifier le niveau de log
    new_level=$(echo -e "trace\ndebug\ninfo\nwarn\nerror\nfatal" | fzf --prompt="Select log level (current: $current_level): ")

    if [[ -n "$new_level" ]]; then
      # Modifier la variable RUST_LOG avec le nouveau niveau pour ce module
      if echo "$RUST_LOG" | grep -q "$module_name"; then
        RUST_LOG=$(echo "$RUST_LOG" | sed "s|$module_name=[^,]*|$module_name=$new_level|")
      else
        RUST_LOG="$RUST_LOG,$module_name=$new_level"
      fi
    fi
  fi
}

# Réinitialiser la configuration
reset_config() {
  RUST_LOG="info"
  echo "RUST_LOG reset to default: info"
}

# Main
case $1 in
  add)
    add_module
    ;;
  show)
    show_config
    ;;
  reset)
    reset_config
    ;;
  *)
    echo "Usage: $0 {add|show|reset}"
    ;;
esac

# Exporter la variable RUST_LOG dans l'environnement actuel du shell
export RUST_LOG
