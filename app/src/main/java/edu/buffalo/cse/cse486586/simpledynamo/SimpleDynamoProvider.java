package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String[] REMOTE_PORT = {"11124", "11112", "11108", "11116", "11120"};
	static final String[] Nodelist = {"5562", "5556", "5554", "5558", "5560"};
	static final int SERVER_PORT = 10000;
	String portStr;
	String myPort;
	String firstSuccessor = null;
	String secondSuccessor = null;
	String firstPredecessor = null;
	String secondPredecessor = null;
	String failurePort = null;
	public Uri mUri = null;
	final Uri uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	static final String KEY = "key";
	static final String VALUE = "value";
	int Flag = 0;
	Boolean flag = false;
	static final String[] columnNames = {KEY, VALUE};
	static MatrixCursor starCursor = new MatrixCursor(columnNames);
	static MatrixCursor queryCursor = new MatrixCursor(columnNames);

	public static Map<String, String> keyValue = new ConcurrentHashMap<String, String>();
	public static Map<String, String> queryMap = new ConcurrentHashMap<String, String>();


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		String coordinator = null;
		if (selection.equals("@")){
			String[] files = getContext().fileList();
			for(String file: files){
				getContext().deleteFile(file);

			}
			return 1;
		}else if (selection.equals("*")){
			String[] files = getContext().fileList();
			for(String file: files){
				getContext().deleteFile(file);
			}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "starDelete", selection);

            return 1;
		}else{

			getContext().deleteFile(selection);

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key;
		String value;
		String coordinator = null;
		FileOutputStream fos;

//		synchronized (this){}

		try{
			key = values.get(KEY).toString();
			value = values.get(VALUE).toString();

//			for (String port: REMOTE_PORT) {

				String predecessor = getPredecessor(String.valueOf(Integer.parseInt(myPort)/2));
				String[] successors = getSuccessor(portStr);


			    coordinator = Lookup(key);
//			    Log.v(TAG,"hash value" + coordinator);
				if (coordinator.equals(portStr)) {

//					if (port.equals(myPort)) {
					Log.w(TAG,"Inserting key @"+  portStr + " : "+ key + " with hash : "+genHash(key));
					fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
					fos.write(value.getBytes());
					fos.close();



//					} else {
//						Log.w(TAG,"Insert Task to"+ myPort +" for key : "+key+" with hash : "+genHash(key));
//						new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", port, key, value);
//					}

//					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertSuccessors", port, key, value);

//					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertSuccessor2", successors[1],key, value);
				}else{
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insert", coordinator, key, value);

				}
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "insertSuccessors", coordinator, key, value);
			    Log.w(TAG, "Insert Task to" + coordinator + " for key : " + key + " with hash : " + genHash(key));


		}catch (NullPointerException e){
			e.printStackTrace();
		}catch(IOException e){
			Log.e(TAG, "Insert Exception" + e.getMessage());
		}catch(Exception e){
			Log.e(TAG, "Insert Exception" + e.getMessage());
		}
		Log.v("insert", values.toString());
		return uri;

	}

	private String Lookup(String key) {
		try {

			String hashkey = genHash(key);

			if(genHash(Nodelist[0]).compareTo(hashkey) >=0 || genHash(Nodelist[4]).compareTo(hashkey)<0){
				return Nodelist[0];
			}else if(genHash(Nodelist[1]).compareTo(hashkey)>=0 && genHash(Nodelist[0]).compareTo(hashkey)<0 ){
				return Nodelist[1];
			}else if(genHash(Nodelist[2]).compareTo(hashkey)>=0 && genHash(Nodelist[1]).compareTo(hashkey)<0 ){
				return Nodelist[2];
			}else if(genHash(Nodelist[3]).compareTo(hashkey)>=0 && genHash(Nodelist[2]).compareTo(hashkey)<0 ){
				return Nodelist[3];
			}else if(genHash(Nodelist[4]).compareTo(hashkey)>=0 && genHash(Nodelist[3]).compareTo(hashkey)<0 ){
				return Nodelist[4];
			}

		}catch(Exception e){
			Log.e(TAG, "LookUp Error " + e.getMessage());
		}

		return null;
	}


	private String keyLookup(String key, String port, String predecessor) {
		try {
			String keyHash = genHash(key);
			String myHash = genHash(port);
			String preHash = genHash(predecessor);

			if (preHash.compareTo(myHash) > 0 && (keyHash.compareTo(myHash) < 0 || keyHash.compareTo(preHash) > 0)){
				return predecessor;
			}else if (keyHash.compareTo(myHash) < 0 && keyHash.compareTo(preHash) >= 0){
				return predecessor;
			}else if (keyHash.compareTo(myHash) == 0 && keyHash.compareTo(preHash) == 0){
				return port;
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));


        String[] successor = getSuccessor(portStr);
		firstSuccessor = successor[0];
		secondSuccessor = successor[1];
		firstPredecessor = getPredecessor(portStr);
		secondPredecessor = getPredecessor2(portStr);

		try {

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		String[] files=getContext().fileList();

			for (String file : files) {
				getContext().deleteFile(file);
			}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "sync", myPort);

		return false;
	}

	private String getPredecessor2(String portStr) {
		if (portStr.equals("5562")) return "5558";
		if (portStr.equals("5556")) return "5560";
		if (portStr.equals("5554")) return "5562";
		if (portStr.equals("5558")) return "5556";
		if (portStr.equals("5560")) return "5554";
		return null;
	}

	private String getPredecessor(String portStr) {
		if (portStr.equals("5562")) return "5560";
		if (portStr.equals("5556")) return "5562";
		if (portStr.equals("5554")) return "5556";
		if (portStr.equals("5558")) return "5554";
		if (portStr.equals("5560")) return "5558";
		return null;
	}

	private String[] getSuccessor(String portStr) {
		if (portStr.equals("5562")) return new String[]{"5556", "5554"};
		if (portStr.equals("5556")) return new String[]{"5554", "5558"};
		if (portStr.equals("5554")) return new String[]{"5558", "5560"};
		if (portStr.equals("5558")) return new String[]{"5560", "5562"};
		if (portStr.equals("5560")) return new String[]{"5562", "5556"};
		return null;
	}

	private String getMyPort(String portStr){
		if (portStr.equals("5554")) return "11108";
		if (portStr.equals("5556")) return "11112";
		if (portStr.equals("5558")) return "11116";
		if (portStr.equals("5560")) return "11120";
		if (portStr.equals("5562")) return "11124";
		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		String msgContent = "";
		FileInputStream fis;

		if (selection.equals("@")){
			MatrixCursor matrixCursor = new MatrixCursor(columnNames);
			String[] files = getContext().fileList();
			for (String file: files){
				try {
					fis = getContext().openFileInput(file);
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					msgContent = br.readLine();
					br.close();
					fis.close();
				}catch (Exception e){
					Log.e(TAG, "file read failed");
				}

				String[] row = {file, msgContent};
				matrixCursor.addRow(row);
			}
			return matrixCursor;

		}
		else if (selection.equals("*")){

//			for (String key: keyValue.keySet()){
//				starCursor.addRow(new String[]{key, keyValue.get(key)});
//			}
			for (String file : getContext().fileList()) {
				try {
					fis = getContext().openFileInput(file);
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					msgContent = br.readLine();
					br.close();
					fis.close();

				} catch (IOException e) {
					Log.e(TAG, "STarQuery exception" + e.getMessage());
				} catch (NullPointerException e) {
					Log.e(TAG, "STarQuery exception" + e.getMessage());
				}
				String[] row = {file, msgContent};
				starCursor.addRow(row);
			}

			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "starQuery", myPort);

			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {

			}
//			Log.w(TAG, "firstPort " + myPort);

			Log.w(TAG,"finalMap-->" + keyValue);

        return starCursor;
		}else {

//			queryCursor = new MatrixCursor(columnNames);

			try {

//				queryMap.clear();

//				for (String port : REMOTE_PORT) {

					String predecessor = getPredecessor(portStr);

//					if (keyLookup(selection, portStr, predecessor)) {

				        String holder = Lookup(selection);

						if (holder.equals(portStr)) {
							fis = getContext().openFileInput(selection);
							BufferedReader br = new BufferedReader(new InputStreamReader(fis));
							msgContent = br.readLine();
							br.close();
							fis.close();

//							Log.w(TAG, "Query processed successfully for key :" + selection + " with value :" + msgContent );

							String[] row = {selection, msgContent};
							queryCursor = new MatrixCursor(columnNames);
							queryCursor.addRow(row);

						}else {
							Log.w(TAG, "Query Task to be passed to "+ holder +" for key :"+selection);

							new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Query", holder, selection, myPort);

							while(true) {
								if (queryMap.get(selection) != null) {
									break;
								}
							}
						}

				Log.w(TAG, "queryMap2 " + queryMap.size());
//				Log.w(TAG,"finalMap--0000>" + keyValue.size());
//				Log.w(TAG, "queryMap2 " + queryMap.toString());
				queryMap.clear();
				return queryCursor;

			} catch (IOException e) {
				Log.w(TAG," Query IOexception"+ e.getMessage());
			} catch (NullPointerException e) {
				Log.w(TAG," Query NullPointerException"+ e.getMessage());
			} catch (Exception e){
				Log.w(TAG," Query Exception"+ e.getMessage());
			}
		}
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }


	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];

			try {
				while (true){

					Socket clientSocket = serverSocket.accept();
					InputStream is = clientSocket.getInputStream();
//					DataInputStream dis = new DataInputStream(is);
					BufferedReader br = new BufferedReader(new InputStreamReader(is));
					String s = br.readLine();
//					Log.i(TAG, "------------------------>" + s);
					String key;
					String value = null;
					String[] msgs = s.split("\t");
					String msg = msgs[0];

					if (msg.equals("insert")){
//						"insert" + key + value
						try{
							key = msgs[1];
							value = msgs[2];
							FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                            fos.write(value.getBytes());
							fos.flush();
							fos.close();
//							DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream());
//							dos.writeUTF("insert success");
//							dos.flush();

						}catch (IOException e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}catch (NullPointerException e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}catch (Exception e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}

					}

					if (msg.equals("insertSuccessors")){
//						"insertSuccessors" + key + value
						try {

							key = msgs[1];
							value = msgs[2];
							FileOutputStream fos = getContext().openFileOutput(key, Context.MODE_PRIVATE);
							fos.write(value.getBytes());
							fos.close();

						}catch (IOException e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}catch (NullPointerException e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}catch (Exception e){
							Log.e(TAG,"Insert Exception"+e.getMessage());
						}

					}

					if (s.contains("starQuery")){

//						"starQuery" + myPort (input)
						String queryResult = "";
						String myport = msgs[1];
//						Log.w(TAG,"query @ ---> " + myport + "port --->" + myPort);
						Log.w(TAG, "receive--->" + s);

						MatrixCursor matrixCursor = new MatrixCursor(columnNames);

						for (String str : getContext().fileList()) {

							try {
								FileInputStream fis = getContext().openFileInput(str);
								BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));

								value = bufferedReader.readLine();
								Log.w(TAG, "key " + str + " query value " + value);

//								queryResult += str +"~"+value+"\t";

								matrixCursor.addRow(new String[]{str, value});

							} catch (FileNotFoundException e) {
								e.printStackTrace();
							}
						}


						while (matrixCursor.moveToNext()) {
							int index = matrixCursor.getColumnIndex(KEY);
							int valueRow = matrixCursor.getColumnIndex(VALUE);
							key = matrixCursor.getString(index);
							value = matrixCursor.getString(valueRow);

							queryResult += key + "~" + value + "\t";
//							Log.w(TAG, "cursor keyValue @"+ myport + "\t" + msgs);
							returnStarQuery(myport, queryResult);
						}

					}

					if (msg.equals("returnStarQuery")){

						Log.w(TAG,"success return*--->" + s);

							for (int i = 1; i < msgs.length; i++) {
								String[] result = msgs[i].split("~");
								key = result[0];
								value = result[1];
								keyValue.put(key, value);
								starCursor.addRow(new String[]{key, value});
							}
						Log.w(TAG, "size " + keyValue.size());

					}

					if (msg.equals("Query")){

//						 "Query", selection, myport
						MatrixCursor matrixCursor = new MatrixCursor(columnNames);
						String selection = msgs[1];
						String myport = msgs[2];
						String valueMsg = null;
//						Log.w(TAG,"Query key :"+ selection);

						try {

							FileInputStream fis = getContext().openFileInput(selection);
							BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));

							valueMsg = bufferedReader.readLine();
//							Log.w(TAG, "key " + selection + " query value " + valueMsg);
							br.close();
							fis.close();

							matrixCursor.addRow(new String[]{selection, valueMsg});

						} catch (FileNotFoundException e) {
							e.printStackTrace();
						}

//						matrixCursor.addRow(new String[]{selection, valueMsg});

						if (matrixCursor.moveToFirst()) {
							int index = matrixCursor.getColumnIndex(KEY);
							int valueRow = matrixCursor.getColumnIndex(VALUE);
							key = matrixCursor.getString(index);
							value = matrixCursor.getString(valueRow);

							String msgToSend =  "returnQuery" + "\t" + key + "\t" + value + "\t" + myport;
//							Log.w(TAG, "cursor keyValue @"+ myport + "\t" + msgs);
							returnQuery(msgToSend);
						} else {
							Log.w(TAG, "No result @" + portStr);
						}

					}

					if (msg.equals("returnQuery")){

						key = msgs[1];
						value = msgs[2];

						Log.w(TAG,"returnQuery_key--->"+ key + " value-->" + value);

						queryMap.clear();
						queryMap.put(key, value);
						queryCursor = new MatrixCursor(columnNames);
						queryCursor.addRow(new String[]{key, value});
//						Log.w(TAG, "queryMap1 " + queryMap.size());
//						Flag = true;

					}

					if (msg.equals("starDelete")){
						for(String file: getContext().fileList()){
							getContext().deleteFile(file);
						}

					}

					if (msg.equals("delete")){
						String selection = s.split("\t")[1];

						getContext().deleteFile(selection);

					}

					if (msg.equals("sync")){

						String predecessorFirst = getPredecessor(String.valueOf(Integer.parseInt(msgs[1]) / 2));
						String predecessorSecond = getPredecessor2(String.valueOf(Integer.parseInt(msgs[1]) / 2));
						String[] successorFirst = getSuccessor(String.valueOf(Integer.parseInt(msgs[1]) / 2));
						MatrixCursor matrixCursor = new MatrixCursor(columnNames);
						String msgToSend = "";

//						Log.w(TAG,"failure--->" + msgs[1]);

						for (String file: getContext().fileList()){

							String holderPort = Lookup(file);
							Log.w(TAG,"coordinator--->" + holderPort);

							 if (holderPort.equals(msgs[1])||predecessorFirst.equals(holderPort)||successorFirst[0].equals(holderPort)){

								 FileInputStream fis = getContext().openFileInput(file);
								 BufferedReader brd = new BufferedReader(new InputStreamReader(fis));
								 String msgValue = brd.readLine();
//								 msgToSend += file + "~" + msgValue + "\t";

								 matrixCursor.addRow(new String[]{file, msgValue});

							 }

						}

						while (matrixCursor.moveToNext()) {
							int index = matrixCursor.getColumnIndex(KEY);
							int valueRow = matrixCursor.getColumnIndex(VALUE);
							key = matrixCursor.getString(index);
							value = matrixCursor.getString(valueRow);

							msgToSend +=  key + "~" + value + "\t";
							Log.w(TAG, "cursor keyValue-->" + msgToSend);
//							returnQuery(msgToSend);
						}

						synchronize(msgs[1], msgToSend);

					}

					if (msg.equals("finalSync")){

						String[] str = s.split("\t");
						insertIntoContent(s);
						Log.w(TAG, "finalSyn-->" + s);
					}

				}
			} catch (Exception e){
				Log.e(TAG, " Server Task Exception :" + e.getMessage());
				e.printStackTrace();
			}

			return null;
		}
	}

	private void synchronize(String port, String str) {

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,"finalSync", port, str);
	}

	private void returnStarQuery(String port, String str) {

		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "returnStarQuery", port, str); //"XXX", port, str = key1 + value1 + ... + keyn + valuen
	}

	private void returnQuery(String str) {
		String[] msgs = str.split("\t");
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgs[0],msgs[1],msgs[2],msgs[3]); // returnQuery + key + value + port;
	}

	private class ClientTask extends  AsyncTask <String, Void, Void> {

		@Override
		protected Void doInBackground(String... msgs) {

			String msg = msgs[0];

			if (msg.equals("insert")){

//				"insert", coordinator, key, value
				String portNo = msgs[1];
				String key = msgs[2];
				String value = msgs[3];
				Log.w(TAG,"received msg to insert at "+ portNo +" for key"+ key);

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portNo) * 2), 5000);

					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println("insert" + "\t" + key + "\t" + value);
					ps.flush();
					socket.close();

				}catch (SocketTimeoutException e){
					e.printStackTrace();
				}catch (IOException e){
					e.printStackTrace();
				}

			}

			if (msg.equals("insertSuccessors")){
				String coordinator = msgs[1];
				String key = msgs[2];
				String value = msgs[3];

				String[] successors = getSuccessor(String.valueOf(Integer.parseInt(coordinator)));

				for(String str : successors) {

					try {

						Socket socket = new Socket();
						socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(str) * 2), 5000);
//						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						PrintStream ps = new PrintStream(socket.getOutputStream());

						ps.println("insertSuccessors" + "\t" + key + "\t" + value);
						Log.w(TAG, "Insert Replicate Client Task msg to " + str
								+ " :" + key + "\t" + value);
						ps.flush();
						socket.close();

					}catch (UnknownHostException e){
						Log.e(TAG,"Replicate UnknownException : "+e.getMessage());
					}catch (IOException e){
						Log.e(TAG,"Replicate IOException : "+e.getMessage());

					}catch (Exception e){
						Log.e(TAG,"Replicate Exception : "+e.getMessage());
					}
				}
			}

			if (msg.contains("starQuery")){
				String portNo = msgs[1];
				String sq = "";
//				Log.w(TAG,"starQuery on client -->" + msgs[0]);
				// "starQuery", myPort or str

				try {
					for (int i = 0; i < REMOTE_PORT.length; i++) {
//						if (!REMOTE_PORT[i].equals(portNo)) {
							Socket socket = new Socket();
							socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(REMOTE_PORT[i])), 5000);
//					        socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portNo)), 1500);
//							DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
							PrintStream ps = new PrintStream(socket.getOutputStream());
							ps.println("starQuery\t" + msgs[1]);
					        Log.w(TAG, "Star Query Client Task to " + portNo);
							ps.flush();
//						}else{
//                            break;
					}
//
				} catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (Exception e){
					e.printStackTrace();
				}

			}

			if (msg.equals("returnStarQuery")){

//				"returnStarQuery", port, queryResult (key1~value1 + "\t" + ... + keyn~valuen)
//				port --> ,msgs[1], queryresult --->msgs[2]
				try {
					String returnResult = "returnStarQuery" + "\t" + msgs[2];
//					Log.w(TAG,"Return star Query to "+msgs[1]+" :"+ returnResult);
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(msgs[1])), 5000);
//					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println(returnResult);
					ps.flush();
//					Log.w(TAG, "good--->" + msgs[0] + "\t" + returnResult);
					Log.w(TAG, "good--->" + msgs[1] + "\t" + returnResult);
					socket.close();

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (msg.equals("Query")){

//				"Query", holdeport, selection, myPort

				String portNo = msgs[1];
				String key = msgs[2];
				String myport = msgs[3];
				Log.w(TAG,"sending to PortNo" + portNo + " for key " + msgs[2]);

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(portNo)*2), 5000);
					Log.w(TAG, "Query Client Task to " + msgs[1] + " :" + "query" + myport);
//					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println("Query" + "\t" + key + "\t" + myport);
					ps.flush();
					socket.close();

				} catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

				String[] successors = getSuccessor(String.valueOf(Integer.parseInt(portNo)));

				try {
					for (String str: successors) {
						Socket socket = new Socket();
						socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(str)*2), 5000);
						Log.w(TAG, "Query Successor to " + msgs[1] + " :" + "query" + str);
//						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						PrintStream ps = new PrintStream(socket.getOutputStream());
						ps.println("Query" + "\t" + key + "\t" + myport);
						ps.flush();
						socket.close();
					}

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Query mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if (msg.equals("returnQuery")){
//				returnQuery +  key + value + myport;
				String key = msgs[1];
				String value = msgs[2];
				String myport = msgs[3];

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(myport)), 5000);
//					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println("returnQuery" + "\t" + key + "\t" + value);
					Log.w(TAG, "returnQuery @" + myport + " key " + key + " value " + value);
					ps.flush();
					socket.close();

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in returnQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if (msg.equals("starDelete")){
				String selection = msgs[1];

				try {
                    for (String port: REMOTE_PORT) {
						Socket socket = new Socket();
						socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), 5000);
//						DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
						PrintStream ps = new PrintStream(socket.getOutputStream());
						ps.println("starDelete");
						ps.flush();
						socket.close();
					}

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if (msg.equals("delete")){
				String selection = msgs[1];
				String coordinator = msgs[2];

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(coordinator)));
//					DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println("delete" + "\t" + selection);
					ps.flush();
					socket.close();

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

			if (msg.equals("sync")){

				String originport = msgs[1]; // originport ---> myPort
				Log.w(TAG, "SyncPort-->" + msgs[1]);

				try {
					for (int i = 0; i < REMOTE_PORT.length; i++){
						Socket socket = new Socket();
						socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(originport)), 5000);
						PrintStream ps = new PrintStream(socket.getOutputStream());
						ps.println("sync\t" + originport);
						ps.close();
						socket.close();

					}
				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (msg.equals("finalSync")){
				String port = msgs[1];

				try {
					Socket socket = new Socket();
					socket.connect(new InetSocketAddress("10.0.2.2", Integer.parseInt(port)), 5000);
					PrintStream ps = new PrintStream(socket.getOutputStream());
					ps.println("finalSync\t" + msgs[2]);

				}catch (SocketTimeoutException e) {
					Log.d(TAG, "Inside catch block in StarQuery mode");
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
//
			return null;
		}
	}

	private void insertIntoContent(String sq) {

		String[] result = sq.split("\t");
		for (int i = 1; i < result.length; i++) {
			String[] s = result[i].split("~");
			String key = s[0];
			String value = s[1];
//			keyValue.put(key, value);
//			starCursor.addRow(new String[]{key, value});
			try {
				FileOutputStream fos = getContext().openFileOutput(key,Context.MODE_PRIVATE);
				fos.write(value.getBytes());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e){
				e.printStackTrace();
			}

		}

		Log.w(TAG, "finalresult-->" + keyValue);
	}

	public Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
}