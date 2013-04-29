/**
 * Esta clase forma parte del conjunto de pr&#225cticas que se desarrollan en la
 * asignatura Redes y Servicios de Telecomunicaci&#243n relacionadas con la 
 * implementaci&#243n de protocolos did&#225cticos.
 * 
 * Universidad Polit&#233cnica de Madrid
 * EUIT de Telecomunicaci&#243n
 * Diatel (2013)
 */
package rrsst.didacCom;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Arrays;
import rrsst.didacCom.IDUDidacCom.*;

/**
 * Esta clase modela el comportamiento del nivel de comunicaciones DidacCOM.
 * 
 * @author Redes y Servicios de Telecomunicaci&#243n
 * @version 3.0
 *
 */
public class DidacComImpl implements IDidacCom 
{
    private DatagramSocket canal = null;
	
    /**
    * Longitud minima en bytes de una PDU.
    */
    public static final int MIN_LONG_PDU = 2 + GestorHash.LONG_HASH;
	
    /**
    * Valor del campo <i>tipo de PDU</i> que permite hacer referencia a una PDU
    * de datos.
    */
    public static final byte PDU_DATOS = 0;
    
    /**
    * Valor del campo <i>tipo de PDU</i> que permite hacer referencia a una PDU
    * ACK.
    */
    public static final byte PDU_ACK = (byte) 0xFE;
    
    /**
    * Valor del campo <i>tipo de PDU</i> que permite hacer referencia a una PDU
    * NACK.
    */
    public static final byte PDU_NACK = (byte) 0xFF;
	
    /**
    * Longitud m&#225xima en bytes del campo de datos de una PDU de datos del
    * nivel DidacCOM.
    */
    public static final int MAX_DATOS_PDU = IDidacCom.MAX_DATOS;

    /**
    * Longitud m&#225xima en bytes de una PDU del nivel DidacCOM.
    */
    public static final int MAX_LONG_PDU = 2+MAX_DATOS_PDU+GestorHash.LONG_HASH;
	
    /**
    * M&#225ximo n&#250mero de reintentos de env&#237o de una PDU del nivel 
    * DidacCOM.
    */	
    private static final int N_REINTENTOS = 3;

    @Override
    public void enviar(IDUDidacCom idu) 
            throws ExcepcionDidacCom, IllegalArgumentException
    {
        /*int tries= 0;
        if (canal== null)
        {
            throw new ExcepcionDidacCom("No se ha abierto el canal");   
        }
        try{
        do{
            int puerto = idu.getPuertoUDP();//Puerto contenido en la IDU
            byte[] datos = idu.getDatos();  //Byte de datos contenidos en la IDU
            int longDatos = idu.getLongDatos(); //Longuitud que tienen los datos
            String IP = idu.getDirIP();     //String IP contenida en la IDU
            
            //Se traduce el String IP a un InetAdress
            DatagramPacket datagrama = new DatagramPacket(datos, longDatos, 
                                            InetAddress.getByName(IP), puerto);
            canal.send(datagrama);
            }while (tries<=N_REINTENTOS);//&& (pduControl.getTipo() == PDU.PDU_NACK));
        
	}catch(IOException e) 
        {
            throw new ExcepcionDidacCom("Ha habido un error");
        }*/
        
        // Crear Datagrama UDP a enviar o recibir.
	DatagramPacket UDP;
	int reenvios = 0; //Contador de reenvios
	boolean transCorrecta = false; //
		
	// Crear el stream contenedor de la PDU a enviar.
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	DataOutputStream filtroOut = new DataOutputStream(baos);
	byte[] cont; //PDU sin código de verificación (hash)
	byte[] PDU; //PDU a enviar
				
	// Crear el stream contenedor de la PDU de confirmación a recibir.
	byte[] confirm = new byte[MIN_LONG_PDU]; //Confirmación de la PDU (ACK o NACK) por lo que será 18
		
	// Validar que el canal se ha creado.
	try{
	    if (canal == null)
	    	throw new ExcepcionDidacCom("No se ha abierto el canal");   
        
	// Inicializar el contador de reenvíos.
	    	
		
	// Validar que el parametro idu es correcto.
	    if (idu == null)
	    	throw new ExcepcionDidacCom("La IDU no es correcta.");

	// Construir la PDU de datos del nivel DidacCom
	// lanzando las excepciones oportunas
	    if(idu.getLongDatos() < 0 || idu.getLongDatos() > MAX_DATOS_PDU)
		throw new ExcepcionDidacCom("Longitud de datos de la PDU incorrecta");
			
	    filtroOut.writeByte(PDU_DATOS);
	    filtroOut.write(idu.getLongDatos());
	    filtroOut.write(idu.getDatos());
	    filtroOut.flush();
	    
	    cont = baos.toByteArray();
			
	    if(cont.length < MIN_LONG_PDU || cont.length > MAX_LONG_PDU)
		throw new ExcepcionDidacCom("Longitud de la PDU incorrecta");
      	    if(GestorHash.generarHash(cont) == null)
		throw new ExcepcionDidacCom("Código de verificación incorrecto");
		
	    filtroOut.write(cont);
	    filtroOut.write(GestorHash.generarHash(cont));
	    filtroOut.flush();
			
	    PDU = baos.toByteArray();	
		
	// Entrar en el bluce de envio y recepción
	// Salir cuando se reciba un ACK (transmisión correcta)
	// o cuando se haya sobrepasado el número máximo de reenvios
	// por haber recibido sucesivos NACKs (transmisión incorrecta)
		
	// *******************************************************************************************
		do{
	
		// Contruir el datagrama que contenga la PDU de datos del nivel DidacCom
		// lanzando las excepciones oportunas
			UDP = new DatagramPacket(PDU, PDU.length,InetAddress.getByName(idu.getDirIP()),idu.getPuertoUDP());
	
		// Enviar el datagrama a través del canal
		// lanzando las excepciones oportunas
			canal.send(UDP);
				
		// Contruir el datagrama que contendrá la PDU ACK o NACK procedente del servidor
		// lanzando las excepciones oportunas	
			DatagramPacket confirmacion = new DatagramPacket(confirm, MIN_LONG_PDU);
			
		// Recibir el datagrama de respuesta través del canal
		// lanzando las excepciones oportunas
			canal.receive(confirmacion);

		
		// Procesar la PDU recibida:
			
		// Comprobar que su longitud está dentro del rango permitido
		// lanzando las excepciones oportunas

			
		// Crear el stream de entrada que contendrá los bytes de la PDU recibida

			
		// Comprobar que el hash de la PDU recibida es correcto
		// lanzando las excepciones oportunas
			
		// Comprobar que tipo de la PDU recibida es correcto (debe ser ACK o NACK)
		// lanzando las excepciones oportunas
			
		// Comprobar que la longitud del campo datos de la PDU recibida es coherente con el tipo de pdu
		// lanzando las excepciones oportunas
			
		// *******************************************************************************************
	    	}while(reenvios <= N_REINTENTOS && ! transCorrecta);
			
	    }catch(IOException e){}
	    finally{
		try {
			if(filtroOut != null)
				filtroOut.close();
		} catch (IOException e){}
	    }
	    // Si finalmente se ha salido del bucle por "Transmisión incorrecta"
	    // lanzar las excepciones oportunas
        
        
    }
        
        @Override
    public IDUDidacCom recibir() throws ExcepcionDidacCom
        {
            IDUDidacCom idu;
            byte[] datos = new byte[20];
            DatagramPacket datagrama = new DatagramPacket(datos,20);
            try
            {
                canal.receive(datagrama);
                idu = new IDUDidacCom (datagrama.getAddress().getHostAddress(), 
                        datagrama.getPort(), datagrama.getData(), 
                        datagrama.getLength());
            }catch (IOException e)   {throw new ExcepcionDidacCom("Error en la "
                    + "recepción: " + e.getMessage());}
            return idu;
		
		// Crear Datagrama UDP a enviar o recibir.

		
		// Crear el stream contenedor de la PDU a enviar.

		
		// Crear el stream contenedor de la PDU de confirmaci�n a recibir.

		
		// Validar que el canal se ha creado.

		
		// Inicializar el contador de reenv�os.

		
		// Validar que el parametro idu es correcto.


		// Construir la PDU de datos del nivel DidacCom
		// lanzando las excepciones oportunas

		
		// Entrar en el bluce de envio y recepci�n
		// Salir cuando se reciba un ACK (transmisi�n correcta)
		// o cuando se haya sobrepasado el n�mero m�ximo de reenvios
		// por haber recibido sucesivos NACKs (transmisi�n incorrecta)
		
		// *******************************************************************************************
		
			// Contruir el datagrama que contenga la PDU de datos del nivel DidacCom
			// lanzando las excepciones oportunas

		
			// Enviar el datagrama a trav�s del canal
			// lanzando las excepciones oportunas
			
			
			// Contruir el datagrama que contendr� la PDU ACK o NACK procedente del servidor
			// lanzando las excepciones oportunas		
			
			
			// Recibir el datagrama de respuesta trav�s del canal
			// lanzando las excepciones oportunas
			

		
			// Procesar la PDU recibida:
			
			// Comprobar que su longitud est� dentro del rango permitido
			// lanzando las excepciones oportunas

			
			// Crear el stream de entrada que contendr� los bytes de la PDU recibida

			
			// Comprobar que el hash de la PDU recibida es correcto
			// lanzando las excepciones oportunas
			
			// Comprobar que tipo de la PDU recibida es correcto (debe ser ACK o NACK)
			// lanzando las excepciones oportunas
			
			// Comprobar que la longitud del campo datos de la PDU recibida es coherente con el tipo de pdu
			// lanzando las excepciones oportunas
			
		// *******************************************************************************************
		
		
		// Si finalmente se ha salido del bucle por "Transmisi�n incorrecta"
		// lanzar las excepciones oportunas

    

 
        
		// Validar que el canal se ha creado.
			
		// Inicializar el contador de reenv�os ACKs enviados.
		

		// Entrar en el bluce de recepci�n y env�o
		// Salir cuando se haya sobrepasado el n�mero m�ximo de NACKs enviados
		// o cuando el hash de la PDU recibida sea incorrecto (errores de transmisi�n)
		
		// *******************************************************************************************		
		
			// Recibir el datagrama
			// lanzando las excepciones oportunas
	
			
			// Procesar la PDU recibida:
			
			// Comprobar que su longitud est� dentro del rango permitido
			// lanzando las excepciones oportunas
			

			// Comprobar que el hash de la PDU recibida es correcto
			// lanzando las excepciones oportunas		
			
			
			//Si el hash es correcto
			
				// Se comprueba que la PDU recibida sea de datos y con una longitud coherente
				// lanzando las excepciones oportunas
				// Si todas las comprobaciones son correctas 
				// el tipo de la PDU de respuesta sera ACK.
			
			// Si el hash no es correcto
			
				// el tipo de la PDU de respuesta sera NACK.
			

			// Generar la PDU de respuesta a enviar, ACK o NACK dependiendo de si el
			// hash de la PDU recibida ha sido correcto o no.
			

			// Enviar la PDU de confirmacion a trav�s del canal.

		// *******************************************************************************************		
		
		// Si finalmente se ha salido del bucle por alg�n hash incorrecto
		// lanzar las excepciones oportunas		
		
		
		// Si finalmente se ha salido del bucle por exceder el n�mero de NACKs enviados
		// lanzar las excepciones oportunas		
		
		
		// Si se llega hasta aqu� es que la recepci�n de la PDU de datos ha sido correcta
		// y se ha enviado ACK
		
		// Componer la IDU para nivel superior		
        }        
 
	
    /**
    *
    */
    public DidacComImpl()
    {
        // Inicializar el canal
    }
    
    /*
     * 
     */
    @Override
    public void iniciar() throws ExcepcionDidacCom 
    {
        try   {canal = new DatagramSocket();} 
        catch (SocketException e)   {throw new ExcepcionDidacCom("Error al "
                + "iniciar:\n  " +e.getCause());}
    }

    @Override
    public void iniciar(int puertoLocal) throws ExcepcionDidacCom 
    {
        try   {canal = new DatagramSocket(puertoLocal);} 
	catch (SocketException e) {throw new ExcepcionDidacCom("Error al "
                + "iniciar canal definiendo un puerto:\n  " +e.getCause());}
    }

    @Override
    public void parar() throws ExcepcionDidacCom 
    {
        if (canal != null)   canal.close();
    }
	
	/**
	 * Comprueba si el hash de una PDU es correcto.
	 * 
	 * @param secuencia Secuencia de bytes que contiene la PDU a analizar. 
	 * @param longitud Longitud de la PDU que se pasa como par&#225metro en 
         * <b><i>secuencia</i></b>.
	 * @return Devuelve <i>true</i> si la PDU incluye un campo hash correcto
         * y <i>false</i> en caso contrario.
	 * @throws IOException Se lanza al detectar un error al extraer de la 
         *                     secuencia de bytes los diferentes campos.
	 * @throws ExcepcionDidacCom Se lanza si la estructura de campos de la 
         *                           PDU es incorrecta.
	 */
	private boolean comprobarHash( byte []secuencia, int longitud) 
                                        throws IOException, ExcepcionDidacCom
        {
            if (longitud != secuencia.length)
            {
                throw new IOException("");
            }
            boolean result; //Devolverá true si todo es correcto
            //Dividir 
            byte[] secuenciaData= Arrays.copyOfRange(secuencia, 0, longitud-16);
            byte[] secuenciaHash= Arrays.copyOfRange (secuencia, longitud-16, longitud);
            result = GestorHash.validarHash(secuenciaData, secuenciaHash);
                        
            return result;
	}
}
