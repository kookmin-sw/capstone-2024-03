import React, { useEffect, useState } from 'react';
const { ipcRenderer } = window;

export const handleButtonReload = () => {
    ipcRenderer.send("screen");
};

function ScreenViewer({setDragCoords, setClickCoords}) {
    const [imageSrc, setImageSrc] = useState(null);
    const [isLoading, setIsLoading] = useState(true);
    const [dragStart, setDragStart] = useState(null);
    const [dragEnd, setDragEnd] = useState(null);
    const [isDragging, setIsDragging] = useState(false);


    const handleMouseDown = (event) => {
        const coords = getRealXY(event);
        setDragStart(coords);
        setIsDragging(true);
    };

    const handleMouseUp = async (event) => {
        if (!isDragging) return;
        const coords = getRealXY(event);
        setDragEnd(coords);
        setIsDragging(false);
        const data = {
            top_left_x: dragStart.x,
            top_left_y: dragStart.y,
            bottom_right_x: coords.x,
            bottom_right_y: coords.y,
        };

        try {
            const response = await fetch('http://127.0.0.1:82/vm/vars', {
                method: 'POST',
                headers: {
                    'accept': 'application/json',
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(data)
            });
            const responseData = await response.json();
            console.log(responseData);
        } catch (error) {
            console.error(error);
        }
        setDragCoords({ top_left_x: dragStart.x, top_left_y: dragStart.y, bottom_right_x: coords.x, bottom_right_y: coords.y });
        setClickCoords({ x: dragStart.x, y: dragStart.y });
        alert(`Save variable : ${dragStart.x}, ${dragStart.y} to ${coords.x}, ${coords.y}`);
    };

    const handleMouseMove = (event) => {
        if (!isDragging) return;
        const coords = getRealXY(event);
        setDragEnd(coords);
    };

    const getRealXY = (event) => {
        const imgElement = document.getElementById('uploaded-image');
        const scaleX = imgElement.naturalWidth / imgElement.offsetWidth;
        const scaleY = imgElement.naturalHeight / imgElement.offsetHeight;

        const realX = Math.round(event.nativeEvent.offsetX * scaleX);
        const realY = Math.round(event.nativeEvent.offsetY * scaleY);
        return { x: realX, y: realY };
    };

    useEffect(() => {
        setIsLoading(true);
        ipcRenderer.send("screen");
        ipcRenderer.on("screen", (_, args) => {
            const blob = new Blob([args.screen], { type: 'image/png' });
            const url = URL.createObjectURL(blob);
            setImageSrc(url);
            setIsLoading(false);
        });
    }, []);

    

    return (
        <div style={{ width: '100%', height: '100%' }}>
            <div>{isLoading ? (
                <label>Loading...</label>
            ) : (
                <label>{imageSrc && <img id="uploaded-image" src={imageSrc} alt="Uploaded" style={{ 
                    width: '100%', 
                    height: '100%', 
                    objectFit: 'contain' 
                }}  onMouseDown={handleMouseDown} onMouseUp={handleMouseUp} onMouseMove={handleMouseMove} />}</label>
            )}</div>
        </div>
    );
}

export default ScreenViewer;