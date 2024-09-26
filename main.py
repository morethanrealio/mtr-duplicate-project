import psycopg2
import time
from psycopg2.extras import RealDictCursor
from sshtunnel import SSHTunnelForwarder
from config import SSH_CONFIG, POSTGRES_CONFIG

project_relation = {}
asset_relation = {}
model_relation = {}

def get_new_id(table_name):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(id) FROM {table_name}")
        max_id = cur.fetchone()[0]
        return max_id + 1 if max_id else 1

def db_connect():
    tunnel = SSHTunnelForwarder(
        (SSH_CONFIG['ssh_address'], SSH_CONFIG['ssh_port']),
        ssh_username=SSH_CONFIG['ssh_username'],
        ssh_private_key=SSH_CONFIG['ssh_private_key'],
        remote_bind_address=(POSTGRES_CONFIG['db_host'], POSTGRES_CONFIG['db_port'])
    )
    tunnel.start()

    conn = psycopg2.connect(
        dbname=POSTGRES_CONFIG['db_name'],
        user=POSTGRES_CONFIG['db_user'],
        password=POSTGRES_CONFIG['db_password'],
        host='localhost',
        port=tunnel.local_bind_port
    )

    print('Banco conectado !!!')

    return conn, tunnel

def duplicate_client(new_project_name, old_client_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_client WHERE id = %s", (old_client_id,))
        old_client = cur.fetchone()
        
        new_id = get_new_id("cms_client")
        nametag = new_project_name.lower().replace(" ", "-")
        new_client = (new_id, new_project_name, nametag, None)
        
        cur.execute("""
            INSERT INTO cms_client (id, name, nametag, google_tagmanager)
            VALUES (%s, %s, %s, %s)
        """, new_client)

        conn.commit()
        print("Novo cliente com sucesso.")
        # print(f"Novo cliente criado com ID: {new_id}")
        return new_id, nametag, old_client['nametag']

def duplicate_clientuser(old_client_id, new_client_id, nametag, new_project_name):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_clientuser WHERE client_id = %s", (old_client_id,))
        old_clientuser = cur.fetchone()

        if old_clientuser:
            new_id = get_new_id("cms_clientuser")
            new_username = f"{nametag}@mtr.io"
            new_email = f"{nametag}@mtr.io"
            new_firstname = new_project_name

            new_clientuser = (
                new_id,
                old_clientuser['password'],
                old_clientuser['last_login'],
                new_username,
                old_clientuser['is_superuser'],
                old_clientuser['is_staff'],
                old_clientuser['is_active'],
                old_clientuser['date_joined'],
                old_clientuser['last_name'],
                new_firstname,
                new_email,
                new_client_id
            )

            cur.execute("""
                INSERT INTO cms_clientuser (id, password, last_login, username, is_superuser, is_staff, is_active, date_joined, last_name, first_name, email, client_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, new_clientuser)

        conn.commit()
        print("ClientUser duplicado com sucesso .")
        # print(f"ClientUser duplicado com sucesso para o novo cliente ID: {new_client_id}")

def duplicate_project(new_client_id, old_project_id, nametag, old_nametag):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_project WHERE client_id = %s", (old_project_id,))
        old_projects = cur.fetchall()

        for project in old_projects:
            new_id = get_new_id("cms_project")
            new_url = project['url'].replace(old_nametag, nametag)
            new_project = (
                new_id,
                project['name'],
                new_client_id,
                project['template_id'],
                project['platform'],
                project['final_date'],
                project['start_date'],
                project['active'],
                project['type_platform'],
                new_url,
                project['url_live'],
                project['is_live_active'],
                project['nametag'],
                project['viewer_count_enabled'],
                project['wowza_nametag'],
                project['viewer_count'],
                project['key'],
                project['count_overall'],
                None,
                project['style'],
                project['config_id']
            )
            cur.execute("""
                INSERT INTO cms_project (id, name, client_id, template_id, platform, final_date, start_date, active, type_platform, url, url_live, 
                                                is_live_active, nametag, viewer_count_enabled, wowza_nametag, viewer_count, key, count_overall, google_tagmanager, style, config_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, new_project)
            project_relation[project['id']] = new_id

        conn.commit()
        print("Projetos duplicados com sucesso.")
        # print(f"Projetos duplicados e relação criada: {project_relation}")

def duplicate_assets(new_client_id, old_project_id):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_asset WHERE client_id = %s", (old_project_id,))
        old_assets = cur.fetchall()

        for asset in old_assets:
            new_id = get_new_id("cms_asset")
            new_asset = (
                new_id,
                asset['name'],
                asset['file'],
                new_client_id,
                asset['asset_type']
            )

            cur.execute("""
                INSERT INTO cms_asset (id, name, file, client_id, asset_type)
                VALUES (%s, %s, %s, %s, %s)
            """, new_asset)
            asset_relation[asset['id']] = new_id

        conn.commit()
        print("Assets duplicados com sucesso.")
        # print(f"Assets duplicados e relação criada: {asset_relation}")

def duplicate_button():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_button WHERE project_id IN %s", (tuple(project_relation.keys()),))
        old_buttons = cur.fetchall()

        for button in old_buttons:
            new_id = get_new_id("cms_button")
            new_project_id = project_relation.get(button['project_id'])
            new_asset_id = asset_relation.get(button['asset_id']) if button['asset_id'] else None

            new_button = (
                new_id,
                button['name'],
                button['active'],
                button['url'],
                new_asset_id,
                new_project_id,
                button['html_id']
            )

            cur.execute("""
                INSERT INTO cms_button (id, name, active, url, asset_id, project_id, html_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, new_button)

        conn.commit()
        print("Botões duplicados com sucesso.")

def duplicate_pictures():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_picture WHERE project_id IN %s", (tuple(project_relation.keys()),))
        old_pictures = cur.fetchall()

        for picture in old_pictures:
            new_id = get_new_id("cms_picture")
            new_project_id = project_relation.get(picture['project_id'])
            new_asset_id = asset_relation.get(picture['asset_id']) if picture['asset_id'] else None

            new_pictures = (
                new_id,
                picture['name'],
                picture['active'],
                new_asset_id,
                new_project_id,
                picture['html_id']
            )

            cur.execute("""
                INSERT INTO cms_picture (id, name, active, asset_id, project_id, html_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, new_pictures)

        conn.commit()
        print("Pictures duplicados com sucesso.")

def duplicate_text():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_text WHERE project_id IN %s", (tuple(project_relation.keys()),))
        old_texts = cur.fetchall()

        for text in old_texts:
            new_id = get_new_id("cms_text")
            new_project_id = project_relation.get(text['project_id'])

            # Cria a tupla com os dados do novo registro a ser inserido
            new_texts = (
                new_id,
                text['name'],
                text['active'],
                text['html_id'],
                text['body'],
                new_project_id
            )

            # Insere o novo registro na tabela cms_text
            cur.execute("""
                INSERT INTO cms_text (id, name, active, html_id, body, project_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, new_texts)

        # Confirma a transação no banco de dados
        conn.commit()
        print("Textos duplicados com sucesso.")

def duplicate_models():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_model WHERE project_id IN %s", (tuple(project_relation.keys()),))
        old_models = cur.fetchall()

        for model in old_models:
            new_id = get_new_id("cms_model")
            new_project_id = project_relation.get(model['project_id'])
            novo_gltf_file_id = asset_relation.get(model['gltf_file_id']) if model['gltf_file_id'] else None
            novo_usdz_file_id = asset_relation.get(model['usdz_file_id']) if model['usdz_file_id'] else None
            new_asset_id = asset_relation.get(model['asset_id']) if model['asset_id'] else None

            new_model = (
                new_id,
                model['name'],
                model['active'],
                new_project_id,
                novo_gltf_file_id, 
                novo_usdz_file_id, 
                model['type_mdl'],
                model['html_id'],
                new_asset_id,
                model['order']
            )

            cur.execute("""
                INSERT INTO cms_model (id, name, active, project_id, gltf_file_id, usdz_file_id, type_mdl, html_id, asset_id, "order")
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, new_model)

            model_relation[model['id']] = new_id

        conn.commit()
        print("Models duplicados com sucesso.")

def duplicate_materials():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_material WHERE project_id IN %s", (tuple(project_relation.keys()),))
        materials_antigos = cur.fetchall()

        for material in materials_antigos:
            new_id = get_new_id("cms_material")
            
            new_project_id = project_relation.get(material['project_id'])
            new_model3d_id = model_relation.get(material['model3d_id'])
            novo_alpha_map_id = asset_relation.get(material['alpha_map_id']) if material['alpha_map_id'] else None
            novo_ao_map_id = asset_relation.get(material['ao_map_id']) if material['ao_map_id'] else None
            novo_normal_map_id = asset_relation.get(material['normal_map_id']) if material['normal_map_id'] else None
            novo_roughness_map_id = asset_relation.get(material['roughness_map_id']) if material['roughness_map_id'] else None
            novo_texture_map_id = asset_relation.get(material['texture_map_id']) if material['texture_map_id'] else None
            new_asset_id = asset_relation.get(material['asset_id']) if material['asset_id'] else None

            novo_material = (
                new_id,
                material['name'],
                material['normal_map_intensity'],
                material['ao_map_intensity'],
                material['roughness_map_intensity'],
                material['type_mat'],
                material['color'],
                material['opacity'],
                material['roughness'],
                material['metalness'],
                material['clearcoat'],
                material['clearcoatRoughness'],
                novo_alpha_map_id, 
                novo_ao_map_id, 
                new_model3d_id,
                novo_normal_map_id, 
                new_project_id,
                novo_roughness_map_id, 
                novo_texture_map_id,  
                new_asset_id,
                material['ean'],
                material['link1'],
                material['link2'],
                material['preco'],
                material['preco_promo'],
                material['description'],
                material['reflectivity'],
                material['skin_tone']
            )

            cur.execute("""
                INSERT INTO cms_material (id, name, normal_map_intensity, ao_map_intensity, roughness_map_intensity, type_mat, color, opacity, roughness, metalness, clearcoat, "clearcoatRoughness", alpha_map_id, ao_map_id, model3d_id, normal_map_id, project_id, roughness_map_id, texture_map_id, asset_id, ean, link1, link2, preco, preco_promo, description, reflectivity, skin_tone)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, novo_material)

        conn.commit()
        print("Materials duplicados com sucesso.")

def duplicate_variant():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT * FROM cms_variant WHERE project_id IN %s", (tuple(project_relation.keys()),))
        old_variants = cur.fetchall()

        for variant in old_variants:
            new_id = get_new_id("cms_variant")
        
            new_project_id = project_relation.get(variant['project_id'])
            new_model3d_id = model_relation.get(variant['model3d_id'])
            new_asset_id = asset_relation.get(variant['asset_id']) if variant['asset_id'] else None

            new_variant = (
                new_id,
                variant['name'],
                variant['label'],
                new_asset_id,
                new_model3d_id,
                new_project_id,
                variant['description']
            )

            cur.execute("""
                INSERT INTO cms_variant (id, name, label, asset_id, model3d_id, project_id, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, new_variant)

        conn.commit()
        print("Variantes duplicadas com sucesso.")

if __name__ == "__main__":
    start_time = time.time()

    conn, tunnel = db_connect()
    print()

    try:
        # Inserir aqui o novo nome do projeto
        new_project_name = "Vult VD"
        # new_project_name = "Eudora V2"

        # Inserir aqui o id do cliente que deseja ser duplicado
        old_client_id = 41
        # old_client_id = 26
        
        new_client_id, nametag, old_nametag = duplicate_client(new_project_name, old_client_id)
        duplicate_clientuser(old_client_id, new_client_id, nametag, new_project_name)
        duplicate_project(new_client_id, old_client_id, nametag, old_nametag)
        duplicate_assets(new_client_id, old_client_id)
        duplicate_button()
        duplicate_pictures()
        duplicate_text()
        duplicate_models()
        duplicate_materials()
        duplicate_variant()

    finally:
        conn.close()
        tunnel.stop()