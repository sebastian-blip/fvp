import json
import uuid
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
usuarios_table = dynamodb.Table('usuarios')
transacciones_table = dynamodb.Table('transacciones')
fondos_table = dynamodb.Table('fondos')
sns = boto3.client('sns')

def suscribir_fondo(event, context):
    """Suscribe a un usuario a un fondo."""
    try:
        body = json.loads(event['body'])
        id_usuario = body['id_usuario']
        id_fondo = int(body['id_fondo'])

        # Obtener información del fondo desde DynamoDB
        fondo = fondos_table.get_item(Key={'id_fondo': id_fondo})['Item']

        # Obtener información del usuario
        usuario = usuarios_table.get_item(Key={'id_usuario': id_usuario})['Item']

        # Validar saldo suficiente
        if usuario['saldo'] < fondo['monto_minimo']:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': f"No tiene saldo disponible para vincularse al fondo {fondo['nombre']}."})
            }

        # Actualizar saldo del usuario
        usuario['saldo'] -= fondo['monto_minimo']
        usuarios_table.update_item(
            Key={'id_usuario': id_usuario},
            UpdateExpression="set saldo = :s",
            ExpressionAttributeValues={':s': usuario['saldo']}
        )

        # Agregar el fondo a la lista de fondos del usuario
        usuarios_table.update_item(
            Key={'id_usuario': id_usuario},
            UpdateExpression="set fondos = list_append(fondos, :f)",
            ExpressionAttributeValues={':f': [id_fondo]}
        )

        # Crear registro de la transacción
        id_transaccion = str(uuid.uuid4())
        transacciones_table.put_item(
            Item={
                'id_transaccion': id_transaccion,
                'id_usuario': id_usuario,
                'id_fondo': id_fondo,
                'tipo': 'apertura',
                'monto': fondo['monto_minimo'],
                'fecha': datetime.now().isoformat()
            }
        )

        # Enviar notificación (reemplaza con la lógica real de envío de email/SMS)
        mensaje = f"Se ha suscrito al fondo {fondo['nombre']} con éxito."
        if usuario.get('preferencia_notificacion') == 'email':
            sns.publish(
                TopicArn='arn:aws:sns:tu-region:tu-cuenta:tu-topic-email',  # Reemplaza con el ARN de tu tópico SNS
                Message=mensaje,
                Subject='Suscripción a Fondo',
                MessageAttributes={
                    'email': {'DataType': 'String', 'StringValue': usuario['email']}
                }
            )
        elif usuario.get('preferencia_notificacion') == 'sms':
            sns.publish(
                PhoneNumber=usuario['telefono'],
                Message=mensaje
            )

        return {
            'statusCode': 200,
            'body': json.dumps({'mensaje': 'Suscripción realizada con éxito.'})
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error al suscribir al fondo: {e}"})
        }


def cancelar_fondo(event, context):
    """Cancela la suscripción de un usuario a un fondo."""
    try:
        body = json.loads(event['body'])
        id_usuario = body['id_usuario']
        id_fondo = int(body['id_fondo'])

        # Obtener información del fondo desde DynamoDB
        fondo = fondos_table.get_item(Key={'id_fondo': id_fondo})['Item']

        # Obtener información del usuario
        usuario = usuarios_table.get_item(Key={'id_usuario': id_usuario})['Item']

        # Validar que el usuario está suscrito al fondo
        if id_fondo not in usuario['fondos']:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'El usuario no está suscrito a este fondo.'})
            }

        # Actualizar saldo del usuario
        usuario['saldo'] += fondo['monto_minimo']
        usuarios_table.update_item(
            Key={'id_usuario': id_usuario},
            UpdateExpression="set saldo = :s",
            ExpressionAttributeValues={':s': usuario['saldo']}
        )

        # Eliminar el fondo de la lista de fondos del usuario
        usuario['fondos'].remove(id_fondo)
        usuarios_table.update_item(
            Key={'id_usuario': id_usuario},
            UpdateExpression="set fondos = :f",
            ExpressionAttributeValues={':f': usuario['fondos']}
        )

        # Crear registro de la transacción
        id_transaccion = str(uuid.uuid4())
        transacciones_table.put_item(
            Item={
                'id_transaccion': id_transaccion,
                'id_usuario': id_usuario,
                'id_fondo': id_fondo,
                'tipo': 'cancelacion',
                'monto': fondo['monto_minimo'],
                'fecha': datetime.now().isoformat()
            }
        )

        # Enviar notificación (reemplaza con la lógica real de envío de email/SMS)
        mensaje = f"Se ha cancelado la suscripción al fondo {fondo['nombre']} con éxito."
        if usuario.get('preferencia_notificacion') == 'email':
            sns.publish(
                TopicArn='arn:aws:sns:tu-region:tu-cuenta:tu-topic-email',  # Reemplaza con el ARN de tu tópico SNS
                Message=mensaje,
                Subject='Cancelación de Suscripción a Fondo',
                MessageAttributes={
                    'email': {'DataType': 'String', 'StringValue': usuario['email']}
                }
            )
        elif usuario.get('preferencia_notificacion') == 'sms':
            sns.publish(
                PhoneNumber=usuario['telefono'],
                Message=mensaje
            )

        return {
            'statusCode': 200,
            'body': json.dumps({'mensaje': 'Cancelación realizada con éxito.'})
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error al cancelar la suscripción al fondo: {e}"})
        }


def ver_historial(event, context):
    """Obtiene el historial de transacciones de un usuario."""
    try:
        body = json.loads(event['body'])
        id_usuario = body['id_usuario']

        # Obtener las transacciones del usuario desde DynamoDB
        response = transacciones_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('id_usuario').eq(id_usuario)
        )

        transacciones = response['Items']

        return {
            'statusCode': 200,
            'body': json.dumps(transacciones)
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Error al obtener el historial de transacciones: {e}"})
        }