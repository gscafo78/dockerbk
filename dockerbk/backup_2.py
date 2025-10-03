import subprocess

def ferma_container_con_suffisso(suffisso, container_da_escludere):
    """
    Ferma tutti i container che iniziano con il suffisso, escluso quello passato.
    Ritorna la lista dei container fermati.
    """
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        stdout=subprocess.PIPE,
        text=True,
        check=True
    )
    containers = result.stdout.strip().split('\n')
    da_fermare = [c for c in containers if c.startswith(suffisso) and c != container_da_escludere]
    for nome in da_fermare:
        subprocess.run(["docker", "stop", nome], check=True)
    return da_fermare

def versione_mariadb_inferiore_11(versione):
    """
    Ritorna True se la versione di MariaDB è inferiore a 11.0
    """
    try:
        # Gestisce anche versioni come '10.11.2', '10.5', 'lts', ecc.
        numeri = versione.split('.')
        major = int(numeri[0]) if numeri[0].isdigit() else 0
        return major < 11
    except Exception:
        return False

def get_postgres_user(nome_container):
    """
    Recupera l'utente PostgreSQL dal container (POSTGRES_USER), default 'postgres'.
    """
    try:
        result = subprocess.run(
            ["docker", "exec", nome_container, "printenv", "POSTGRES_USER"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        user = result.stdout.strip()
        return user if user else "postgres"
    except Exception:
        return "postgres"

def get_db_root_password(nome_container, tipo_db):
    """
    Recupera la password di root dal container tramite docker inspect.
    """
    env_var = "MYSQL_ROOT_PASSWORD" if tipo_db == "MySQL" else "MARIADB_ROOT_PASSWORD"
    try:
        result = subprocess.run(
            ["docker", "inspect", nome_container, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        for line in result.stdout.splitlines():
            if line.startswith(env_var + "="):
                return line.split("=", 1)[1]
        return None
    except Exception:
        return None

def get_db_user_password(nome_container, tipo_db):
    """
    Recupera user e password dal container tramite docker inspect.
    Supporta anche le variabili MYSQL_USER/MYSQL_PASSWORD.
    """
    if tipo_db == "MySQL":
        user_var = "MYSQL_USER"
        pass_var = "MYSQL_PASSWORD"
    else:
        user_var = "MARIADB_USER"
        pass_var = "MARIADB_PASSWORD"
    # Fallback su variabili MySQL se quelle MariaDB non sono presenti
    fallback_user_var = "MYSQL_USER"
    fallback_pass_var = "MYSQL_PASSWORD"
    try:
        result = subprocess.run(
            ["docker", "inspect", nome_container, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        user = None
        password = None
        for line in result.stdout.splitlines():
            if line.startswith(user_var + "="):
                user = line.split("=", 1)[1]
            elif line.startswith(pass_var + "="):
                password = line.split("=", 1)[1]
            elif line.startswith(fallback_user_var + "=") and not user:
                user = line.split("=", 1)[1]
            elif line.startswith(fallback_pass_var + "=") and not password:
                password = line.split("=", 1)[1]
        return user, password
    except Exception:
        return None, None

def backup_database(nome_container, tipo_db, file_backup):
    """
    Esegue il backup del database in base al tipo.
    Usa sempre l'utente e la password specificati nelle variabili d'ambiente per MariaDB/MySQL.
    """
    user, password = get_db_user_password(nome_container, tipo_db)
    if not user or not password:
        print("User o password non trovati per il backup MariaDB/MySQL.")
        return False

    password_opt = ["-p" + password]

    if tipo_db == "MySQL" or tipo_db == "MariaDB":
        # Backup solo del database specificato se disponibile
        db_name = None
        try:
            result = subprocess.run(
                ["docker", "inspect", nome_container, "--format", "{{range .Config.Env}}{{println .}}{{end}}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("MYSQL_DATABASE="):
                    db_name = line.split("=", 1)[1]
                elif line.startswith("MARIADB_DATABASE=") and not db_name:
                    db_name = line.split("=", 1)[1]
        except Exception:
            db_name = None

        if tipo_db == "MySQL":
            cmd = [
                "docker", "exec", nome_container,
                "mysqldump", "-u", user, *password_opt, db_name if db_name else "--all-databases"
            ]
        else:  # MariaDB
            cmd = [
                "docker", "exec", nome_container,
                "mariadb-dump", "-u", user, *password_opt, db_name if db_name else "--all-databases"
            ]
    elif tipo_db == "PostgreSQL":
        user_pg = get_postgres_user(nome_container)
        cmd = [
            "docker", "exec", nome_container,
            "pg_dumpall", "-U", user_pg
        ]
    elif tipo_db == "MongoDB":
        cmd = [
            "docker", "exec", nome_container,
            "mongodump", "--archive"
        ]
    else:
        print(f"Backup non implementato per il tipo: {tipo_db}")
        return False

    with open(file_backup, "wb") as f:
        proc = subprocess.run(cmd, stdout=f)
        return proc.returncode == 0

def riavvia_container(lista_container):
    """
    Riavvia i container nella lista.
    """
    for nome in lista_container:
        subprocess.run(["docker", "start", nome], check=True)

def gestisci_backup(nome_container, tipo_db):
    """
    Ferma i container con lo stesso suffisso, fa il backup e li riavvia.
    """
    # suffisso = nome_container.split('-')[0]
    # fermati = ferma_container_con_suffisso(suffisso, nome_container)
    file_backup = f"{nome_container}_{tipo_db}_backup.sql"
    print(f"Eseguo backup su {file_backup}...")
    ok = backup_database(nome_container, tipo_db, file_backup)
    if ok:
        print("Backup completato.")
    else:
        print("Backup fallito.")
    # riavvia_container(fermati)
    print("Container riavviati.")

def versione_reale_mariadb(nome_container):
    """
    Restituisce la versione reale di MariaDB dal container.
    """
    try:
        result = subprocess.run(
            ["docker", "exec", nome_container, "mariadb", "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        # Output esempio: 'mariadb from 11.8.3-MariaDB, client 15.2 for debian-linux-gnu'
        import re
        match = re.search(r'from ([\d\.]+)-MariaDB', result.stdout)
        if match:
            return match.group(1)
    except Exception as e:
        print(f"Errore nel recupero versione MariaDB: {e}")
    return "unknown"

if __name__ == "__main__":
    # Esempio di test
    nome = "firefly_iii_db"
    tipo = "MariaDB"
    gestisci_backup(nome, tipo)