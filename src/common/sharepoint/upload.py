import os

from dotenv import load_dotenv
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

load_dotenv()


def get_sharepoint_context_using_user(url_sharepoint: str):
    """
    Se crea el contexto de sharepoint ingresando
    nuestro usuario y contraseña
    """
    # Get sharepoint credentials
    sharepoint_url = url_sharepoint

    # Initialize the client credentials
    user_credentials = UserCredential(
        os.environ["USER_SHAREPOINT"], os.environ["PASSWORD_SHAREPOINT"]
    )

    # create client context object
    ctx = ClientContext(sharepoint_url).with_credentials(user_credentials)

    return ctx


def create_sharepoint_directory(url_sharepoint: str, dir_name: str):
    """
    Crea directorio en sharepoint especificado en
    'url_sharepoint',con el usuario y contraseña
    definidos en 'get_sharepoint_context_using_user'

    La URL_SHAREPOINT en la web, debe estar en ingles y no en español
    SI : "Shared Documents" , NO: "Documentos Compartidos"
    """
    if dir_name:
        ctx = get_sharepoint_context_using_user(url_sharepoint)

        result = ctx.web.ensure_folder_path(
            f"Shared Documents/{dir_name}"
        ).execute_query()

    if result:
        # documents is titled as Shared Documents for relative URL in SP
        relative_url = f"Shared Documents/{dir_name}"
        return relative_url


def upload_to_sharepoint(url_sharepoint: str,
                         dir_name: str, file_name: str) -> None:
    """
    Sube un archivo 'file_name' a la carpeta
    'dir_name' en el sharepoint 'url_sharepoint'
    """
    sp_relative_url = create_sharepoint_directory(url_sharepoint, dir_name)
    ctx = get_sharepoint_context_using_user(url_sharepoint)

    target_folder = ctx.web.get_folder_by_server_relative_url(sp_relative_url)

    with open(file_name, "rb") as content_file:
        file_content = content_file.read()

    name = os.path.basename(file_name)

    target_file = target_folder.upload_file(name, file_content).execute_query()

    print(
        "Archivo subido exitosamente a la url: {0}".format(
            target_file.serverRelativeUrl
        ))
